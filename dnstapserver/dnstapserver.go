package dnstapserver

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	dnstap "passivedns/dnstap"

	framestream "github.com/farsightsec/golang-framestream"
	protobuf "google.golang.org/protobuf/proto"
)

type DnstapHandler interface {
	Handle(dnstap *dnstap.Dnstap)
}

type DnstapServer interface {
	Listen(reader io.Reader, bidrectional bool, timeout time.Duration)
	Close()
	Wait()
}

func nameOf(handler DnstapHandler) string {
	return strings.TrimLeft(reflect.TypeOf(handler).String(), "*")
}

func New(workers int, queue int, handlers ...DnstapHandler) DnstapServer {
	fmt.Fprintln(os.Stdout, "Creating Dnstap server")
	server := &dnstapserver{wg: new(sync.WaitGroup), pipe: make(chan []byte, queue), mutex: new(sync.RWMutex), running: true, handlers: handlers}

	fmt.Fprintln(os.Stdout, "Registering Dnstap handler(s)")
	for _, handler := range handlers {
		fmt.Fprintf(os.Stdout, "+ %s\n", nameOf(handler))
	}

	fmt.Fprintf(os.Stdout, "Spawning %v Dnstap worker thread(s)\n", workers)
	for i := 0; i < workers; i++ {
		server.wg.Add(1)
		go server.listen(server.pipe)
	}

	return server
}

type dnstapserver struct {
	wg       *sync.WaitGroup
	pipe     chan []byte
	mutex    *sync.RWMutex
	running  bool
	handlers []DnstapHandler
}

func (this *dnstapserver) listen(pipe <-chan []byte) {
	defer this.wg.Done()

	for frame := range pipe {
		dnstap := &dnstap.Dnstap{}
		if e := protobuf.Unmarshal(frame, dnstap); e == nil {
			if this.handlers != nil && 0 < len(this.handlers) {
				for _, handler := range this.handlers {
					handler.Handle(dnstap)
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "protobuf.Unmarshal(...) failed: %s", e)
			break
		}
	}
	fmt.Fprintln(os.Stdout, "Dnstap worker thread terminated")
}

// MaxFrameSize sets the upper limit on input Dnstap payload (frame) sizes. If an Input
// receives a Dnstap payload over this size limit, ReadInto will log an error and return.
//
// EDNS0 and DNS over TCP use 2 octets for DNS message size, imposing a maximum
// size of 65535 octets for the DNS message, which is the bulk of the data carried
// in a Dnstap message. Protobuf encoding overhead and metadata with some size
// guidance (e.g., identity and version being DNS strings, which have a maximum
// length of 255) add up to less than 1KB. The default 96KiB size of the buffer
// allows a bit over 30KB space for "extra" metadata.
const MAXFRAMESIZE uint32 = 96 * 1024

func (this *dnstapserver) redirect(reader *framestream.Reader, pipe chan<- []byte) {
	defer this.wg.Done()

	if reader != nil {
		buffer := make([]byte, MAXFRAMESIZE)
		for this.running {
			if length, e := reader.ReadFrame(buffer); e == nil {
				frame := make([]byte, length)
				if copy(frame, buffer) != length {
					panic(fmt.Sprintf("Something went terribly wrong, failed to copy %v bytes from the receive buffer to a Dnstap frame", length))
				}

				this.mutex.RLock()
				if this.running {
					// write dnstap frame to channel
					pipe <- frame
				}
				this.mutex.RUnlock()
			} else {
				if e != io.EOF {
					fmt.Fprintf(os.Stderr, "Dnstap listener thread encountered the following error \"%v\"\n", e)
				}
				break
			}
		}
	}
	fmt.Fprintln(os.Stdout, "Dnstap listener thread terminated")
}

func (this *dnstapserver) Listen(reader io.Reader, bidirectional bool, timeout time.Duration) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	if this.running {
		if stream, e := framestream.NewReader(reader, &framestream.ReaderOptions{ContentTypes: [][]byte{[]byte("protobuf:dnstap.Dnstap")}, Bidirectional: bidirectional, Timeout: timeout}); e == nil {
			fmt.Fprintln(os.Stdout, "Spawning Dnstap listener thread")
			this.wg.Add(1)
			go this.redirect(stream, this.pipe)
		} else {
			panic(e)
		}
	} else {
		panic("Dnstap server is stopped (closed)\n")
	}
}

func (this *dnstapserver) Close() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.running {
		this.running = false
		fmt.Fprintln(os.Stdout, "Closing the Dnstap server")
		close(this.pipe)
		this.pipe = nil
		fmt.Fprintln(os.Stdout, "Dnstap server closed")
	}
}

func (this *dnstapserver) Wait() {
	this.wg.Wait()
}
