package main

import (
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/signal"
	"passivedns/dnstapserver"
	"runtime"
	"syscall"
	"time"
)

// Version is current version of this program.
var version = v{1, 0, 6, 24}

type v struct {
	Major, Minor, Patch, Build int
}

func (this v) String() string {
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

func hook(callback func(), signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	go func(signals <-chan os.Signal, callback func()) {
		<-signals
		callback()
		os.Exit(0)
	}(c, callback)
}

func address(file string) (string, string) {
	if fstat, e := os.Stat(file); e == nil {
		if fstat.Mode().Type() == fs.ModeSocket {
			os.Remove(file)
		} else {
			fatalf("\"%v\" exists and is not a Unix socket", file)
		}
	}
	return "unix", file
}

type arguments struct {
	file   *string
	text   *bool
	json   *bool
	sqlite *string
}

func parse() arguments {
	arguments := arguments{
		file:   flag.String("socket", "", "Path to DNStap Unix socket"),
		text:   flag.Bool("text", false, "Use text formatted output"),
		json:   flag.Bool("json", false, "Use verbose JSON formatted output"),
		sqlite: flag.String("sqlite", "", "Write to SQLite3 database")}

	flag.Parse()

	if *arguments.file == "" || len(*arguments.file) == 0 {
		fatalln("Missing argument -socket <path>")
	}

	return arguments
}

func handlers(arguments arguments) []dnstapserver.DnstapHandler {
	handlers := []dnstapserver.DnstapHandler{}

	if *arguments.text {
		handlers = append(handlers, NewTextDnstapHander())
	}
	if *arguments.json {
		handlers = append(handlers, NewResolverResponseJsonDnstapHander())
	}
	if *arguments.sqlite != "" && 0 < len(*arguments.sqlite) {
		handlers = append(handlers, NewResolverResponseSqliteDnstapHandler(*arguments.sqlite))
	}

	/*
		if len(handlers) == 0 {
			fatalf("No handler(s) defined [-json, -sqlite]")
		}
	*/

	return handlers
}

func main() {
	fmt.Fprintf(os.Stdout, "PassiveDNS v%s (%v)\n", version, runtime.Version())

	arguments := parse()

	// create the server and spawn worker threads
	server := dnstapserver.New(runtime.NumCPU(), 8*runtime.NumCPU(), handlers(arguments)...)

	// close server on SIGKILL, SIGTERM, and SIGINT
	hook(func() { server.Close() }, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGINT)

	if socket, e := net.Listen(address(*arguments.file)); e == nil {
		defer socket.Close()
		fmt.Fprintf(os.Stdout, "Unix socket \"%v\" successfully opened, waiting for connections\n", *arguments.file)
		for {
			if connection, e := socket.Accept(); e == nil {
				fmt.Fprintln(os.Stdout, "Connection to socket accepted, server is now listening on the established connection")
				server.Listen(connection, true, 10*time.Second)
			} else {
				fmt.Fprintln(os.Stderr, e)
			}
		}
	} else {
		fmt.Fprintln(os.Stderr, e)
	}

	/*
		filename := "/home/magnus/dnstap.protobuf"
		if reader, e := os.Open(filename); e == nil {
			defer reader.Close()

			server.Listen(bufio.NewReader(reader), false, 0)
		}

		server.Wait()
		server.Close()
	*/
}
