package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	dnstap "passivedns/dnstap"
	dnstapserver "passivedns/dnstapserver"
	"reflect"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/miekg/dns"
)

type textDnstapHandler struct {
	//
}

func resolverResponse(dns *dnstap.Dnstap) bool {
	return *dns.Type == dnstap.Dnstap_MESSAGE && dns.Message != nil && *dns.Message.Type == dnstap.Message_RESOLVER_RESPONSE && dns.Message.ResponseMessage != nil
}

func (this *textDnstapHandler) Handle(dnstap *dnstap.Dnstap) {
	fmt.Println(dnstap)
}

func NewTextDnstapHander() dnstapserver.DnstapHandler {
	return &textDnstapHandler{}
}

type Answer struct {
	Time  time.Time `json:"time"`
	Name  string    `json:"name"`
	Ttl   uint32    `json:"ttl"`
	Class string    `json:"class"`
	Type  string    `json:"type"`
	Data  string    `json:"data"`
}

func (this *Answer) Json() string {
	if bytes, e := json.Marshal(this); e == nil {
		return string(bytes)
	} else {
		return ""
	}
}

func answers(dnstap *dnstap.Dnstap) ([]Answer, error) {
	var answers []Answer = make([]Answer, 0, 8)

	var e error = nil
	if resolverResponse(dnstap) {
		msg := new(dns.Msg)
		if e = msg.Unpack(dnstap.Message.ResponseMessage); e == nil {
			if msg.Answer != nil {
				for _, rr := range msg.Answer {
					if rr.Header().Rrtype == dns.TypeA || rr.Header().Rrtype == dns.TypeAAAA || rr.Header().Rrtype == dns.TypeCNAME {
						var answer Answer
						answer.Time = time.Unix(int64(*dnstap.Message.ResponseTimeSec), int64(*dnstap.Message.ResponseTimeNsec))
						answer.Name = rr.Header().Name
						answer.Ttl = rr.Header().Ttl
						answer.Class = dns.ClassToString[rr.Header().Class]
						answer.Type = dns.TypeToString[rr.Header().Rrtype]
						switch rrtype := rr.(type) {
						case *dns.A:
							answer.Data = rrtype.A.String()
						case *dns.AAAA:
							answer.Data = rrtype.AAAA.String()
						case *dns.CNAME:
							answer.Data = rrtype.Target
						default:
							fmt.Fprintf(os.Stderr, "Unhandled RR type \"%s\"", reflect.TypeOf(rr))
							continue
						}
						answers = append(answers, answer)
					}
				}
			}
		}
	}
	return answers, e
}

type resolverResponseJsonDnstapHandler struct {
	//
}

func (this *resolverResponseJsonDnstapHandler) Handle(dns *dnstap.Dnstap) {
	if resolverResponse(dns) {
		if answers, e := answers(dns); e == nil {
			for _, answer := range answers {
				fmt.Println(answer.Json())
			}
			/*
				if answers != nil && 0 < len(answers) {
					if bytes, e := json.Marshal(answers); e == nil {
						fmt.Fprintln(os.Stdout, string(bytes))
					}
				}
			*/
		}
	}
}

func NewResolverResponseJsonDnstapHander() dnstapserver.DnstapHandler {
	return &resolverResponseJsonDnstapHandler{}
}

type resolverResponseSqliteDnstapHandler struct {
	db *sql.DB
}

const ISO8601 string = "2006-01-02 15:04:05.999999"

func (this *resolverResponseSqliteDnstapHandler) insert(answers []Answer) error {
	//this.logger.Printf("Inserting %v answer(s) to database", len(answers))
	if statement, e := this.db.Prepare("INSERT INTO answers (time, name, ttl , class, type, data) VALUES(?, ?, ?, ?, ?, ?)"); e == nil {
		defer statement.Close()

		statement.Exec("BEGIN TRANSACTION;")
		for _, answer := range answers {
			_, e := statement.Exec(answer.Time.Format(ISO8601), strings.TrimRight(answer.Name, "."), answer.Ttl, answer.Class, answer.Type, answer.Data)
			if e != nil {
				statement.Exec("ROLLBACK;")
				if e, ok := e.(sqlite3.Error); ok {
					if e.ExtendedCode != sqlite3.ErrConstraintPrimaryKey && e.ExtendedCode != sqlite3.ErrConstraintForeignKey {
						return e
					}
				} else {
					return e
				}
			}
		}
		statement.Exec("COMMIT;")
	} else {
		fatalln(e)
	}
	return nil
}

func (this *resolverResponseSqliteDnstapHandler) Handle(dns *dnstap.Dnstap) {
	if resolverResponse(dns) {
		if answers, e := answers(dns); e == nil && 0 < len(answers) {
			if e := this.insert(answers); e != nil {
				fatalln(e)
			}
		}
	}
}

func NewResolverResponseSqliteDnstapHandler(database string) dnstapserver.DnstapHandler {
	stderr := log.New(os.Stderr, "", 0)

	fmt.Fprintf(os.Stdout, "Creating SQLite3 callback handler \"%v\"\n", database)

	db, e := sql.Open("sqlite3", database)
	if e != nil {
		stderr.Fatalln(e)
	}

	if _, e := db.Exec(`CREATE TABLE IF NOT EXISTS answers (
		time TEXT NOT NULL,
		name TEXT NOT NULL,
		ttl INTEGER NOT NULL,
		class TEXT NOT NULL,
		type TEXT NOT NULL,
		data TEXT NOT NULL
	);`); e != nil {
		fatalln(e)
	}

	return &resolverResponseSqliteDnstapHandler{db: db}
}
