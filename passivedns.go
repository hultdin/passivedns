package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/signal"
	"passivedns/dnstapserver"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// Version is current version of this program.
var version = Version{1, 7, 8, 4}

type Version struct {
	Major, Minor, Patch, Build int
}

func (this Version) String() string {
	return fmt.Sprintf("%d.%d.%d", this.Major, this.Minor, this.Patch)
}

func fatalf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}

func fatalln(a interface{}) {
	fmt.Fprintln(os.Stderr, a)
	os.Exit(1)
}

func hook(callback func(signal os.Signal), signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	go func(signals <-chan os.Signal, callback func(signal os.Signal)) {
		for {
			callback(<-signals)
		}
	}(c, callback)
}

func address(file string) (string, string) {
	if fstat, e := os.Stat(file); e == nil {
		if fstat.Mode().Type() == fs.ModeSocket {
			os.Remove(file)
		} else {
			fatalf("\"%v\" exists and is not a Unix socket\n", file)
		}
	} else {
		if _, e := os.Stat(filepath.Dir(file)); e != nil {
			fatalf("\"%v\" invalid path\n", filepath.Dir(file))
		}
	}
	return "unix", file
}

type arguments struct {
	input  *string
	text   *bool
	json   *bool
	sqlite *string
}

func parse() arguments {
	arguments := arguments{
		input:  flag.String("input", "", "Path to DNStap Unix socket"),
		text:   flag.Bool("text", false, "Use text formatted output"),
		json:   flag.Bool("json", false, "Use verbose JSON formatted output"),
		sqlite: flag.String("sqlite", "", "Write to SQLite3 database")}

	flag.Parse()

	if *arguments.input == "" || len(*arguments.input) == 0 {
		fatalln("Missing argument -input <file>")
	}

	return arguments
}

func handlers(worker dnstapserver.DnstapWorker, arguments arguments) []dnstapserver.DnstapMessageHandler {
	handlers := []dnstapserver.DnstapMessageHandler{}

	if *arguments.text {
		handlers = append(handlers, NewTextWriterHander(os.Stdout))
	}
	if *arguments.json {
		handlers = append(handlers, NewResolverResponseJsonMessageHandler(os.Stdout))
	}
	if *arguments.sqlite != "" && 0 < len(*arguments.sqlite) {
		handlers = append(handlers, NewResolverResponseSqliteMessageHandler(*arguments.sqlite, 32))
	}

	/*
		if len(handlers) == 0 {
			fatalf("No handler(s) defined [-json, -sqlite]")
		}
	*/

	return handlers
}

func socket(file string) bool {
	if fstat, e := os.Stat(file); e == nil {
		return fstat.Mode().Type() == fs.ModeSocket
	}
	return true
}

func run(server dnstapserver.DnstapServer, file string, timeout time.Duration) {
	if socket(file) {
		// read DNStap frames from a Unix socket
		if socket, e := net.Listen(address(file)); e == nil {
			defer socket.Close()
			fmt.Fprintf(os.Stderr, "Unix socket \"%v\" successfully created, waiting for connections\n", file)
			for {
				if connection, e := socket.Accept(); e == nil {
					fmt.Fprintln(os.Stderr, "Connection to socket accepted")
					server.Read(connection, true, timeout)
					fmt.Fprintln(os.Stderr, "Dnstap server is now listening on the established connection")
				} else {
					fmt.Fprintln(os.Stderr, e)
				}
			}
		} else {
			fmt.Fprintln(os.Stderr, e)
		}
	} else {
		// read DNStap frames from a regular file
		if reader, e := os.Open(file); e == nil {
			defer reader.Close()

			server.Read(bufio.NewReader(reader), false, 0)
		}
		//time.Sleep(5 * time.Second)
	}

}

func main() {
	fmt.Fprintf(os.Stderr, "PassiveDNS v%s (%v)\n", version, runtime.Version())

	arguments := parse()

	// create the server and spawn worker threads
	server := dnstapserver.New(runtime.NumCPU(), 8*runtime.NumCPU(), func(worker dnstapserver.DnstapWorker) []dnstapserver.DnstapMessageHandler {
		return handlers(worker, arguments)
	})

	// stop server on SIGKILL, SIGTERM, and SIGINT
	hook(func(signal os.Signal) { server.Stop() }, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGINT)

	// run the server with the given arguments
	go run(server, *arguments.input, 15*time.Second)

	// wait for the server to finish
	server.Wait()
}
