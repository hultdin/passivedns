package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	dnstap "passivedns/dnstap"
	dnstapserver "passivedns/dnstapserver"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/miekg/dns"
)

type textWriterHandler struct {
	output io.Writer
}

func (this *textWriterHandler) Handle(message *dnstap.Message) {
	this.output.Write([]byte(message.String() + "\n"))
}

func (this *textWriterHandler) Close() {
	//
}

func NewTextWriterHander(output io.Writer) dnstapserver.DnstapMessageHandler {
	return &textWriterHandler{output: output}
}

type Answer struct {
	Id    uint16
	Time  time.Time
	Name  string
	Ttl   uint32
	Class uint16
	Type  uint16
	Data  string
}

func (this *Answer) Json() (string, bool) {
	if bytes, e := json.Marshal(
		struct {
			Id    uint16        `json:"id"`
			Time  time.Time     `json:"time"`
			Name  string        `json:"name"`
			Ttl   uint32        `json:"ttl"`
			Class []interface{} `json:"class"`
			Type  []interface{} `json:"type"`
			Data  string        `json:"data"`
		}{
			Id:    this.Id,
			Time:  this.Time,
			Name:  strings.TrimRight(this.Name, "."),
			Ttl:   this.Ttl,
			Class: []interface{}{this.Class, dns.ClassToString[this.Class]},
			Type:  []interface{}{this.Type, dns.TypeToString[this.Type]},
			Data:  strings.TrimRight(this.Data, "."),
		}); e == nil {
		return string(bytes), true
	} else {
		return "", false
	}
}

// see github.com/miekg/dns/types.go
func data(rr dns.RR) (string, bool) {
	switch rrtype := rr.(type) {
	case *dns.A:
		return rrtype.A.String(), true
	case *dns.AAAA:
		return rrtype.AAAA.String(), true
	case *dns.CNAME:
		return rrtype.Target, true
	case *dns.NS:
		return rrtype.Ns, true
	case *dns.PTR:
		return rrtype.Ptr, true
	case *dns.DNAME:
		return rrtype.Target, true
	case *dns.GPOS:
		return fmt.Sprintf("%s %s %s", rrtype.Longitude, rrtype.Latitude, rrtype.Altitude), true
	case *dns.DLV:
		return fmt.Sprintf("%v %v %v %s", rrtype.KeyTag, rrtype.Algorithm, rrtype.DigestType, strings.ToUpper(rrtype.Digest)), true
	case *dns.CDS:
		return fmt.Sprintf("%v %v %v %s", rrtype.KeyTag, rrtype.Algorithm, rrtype.DigestType, strings.ToUpper(rrtype.Digest)), true
	case *dns.DS:
		return fmt.Sprintf("%v %v %v %s", rrtype.KeyTag, rrtype.Algorithm, rrtype.DigestType, strings.ToUpper(rrtype.Digest)), true
	case *dns.TA:
		return fmt.Sprintf("%v %v %v %s", rrtype.KeyTag, rrtype.Algorithm, rrtype.DigestType, strings.ToUpper(rrtype.Digest)), true
	case *dns.SSHFP:
		return fmt.Sprintf("%v %v %s", rrtype.Algorithm, rrtype.Type, strings.ToUpper(rrtype.FingerPrint)), true
	case *dns.DNSKEY:
		return fmt.Sprintf("%v %v %v %s", rrtype.Flags, rrtype.Protocol, rrtype.Algorithm, rrtype.PublicKey), true
	case *dns.RKEY:
		return fmt.Sprintf("%v %v %v %s", rrtype.Flags, rrtype.Protocol, rrtype.Algorithm, rrtype.PublicKey), true
	case *dns.DHCID:
		return rrtype.Digest, true
	case *dns.TLSA:
		return fmt.Sprintf("%v %v %v %s", rrtype.Usage, rrtype.Selector, rrtype.MatchingType, rrtype.Certificate), true
	case *dns.UID:
		return strconv.FormatInt(int64(rrtype.Uid), 10), true
	case *dns.GID:
		return strconv.FormatInt(int64(rrtype.Gid), 10), true
	case *dns.EID:
		return strings.ToUpper(rrtype.Endpoint), true
	case *dns.NIMLOC:
		return strings.ToUpper(rrtype.Locator), true
	case *dns.OPENPGPKEY:
		return rrtype.PublicKey, true
	case *dns.ZONEMD:
		return fmt.Sprintf("%v %v %v %s", rrtype.Serial, rrtype.Scheme, rrtype.Hash, rrtype.Digest), true
	default:
		// lets call it what it is, it's an ugly hack
		if fields := strings.Fields(rr.String()); 4 < len(fields) {
			return strings.Join(fields[4:], " "), true
		} else {
			return "", false
		}
	}

}

func contains(rrtypes *[]uint16, rrtype uint16) bool {
	if rrtypes != nil {
		for _, t := range *rrtypes {
			if rrtype == t {
				return true
			}
		}
	}
	return false
}

// extact answers from the DNS message
func answers(message *dnstap.Message, types []uint16) ([]Answer, error) {
	var answers []Answer = make([]Answer, 0, 8)
	var e error = nil

	if message != nil && message.ResponseMessage != nil {
		msg := new(dns.Msg)
		if e = msg.Unpack(message.ResponseMessage); e == nil {
			if msg.Rcode == dns.RcodeSuccess && msg.Answer != nil {
				for _, rr := range msg.Answer {
					if len(types) == 0 || contains(&types, rr.Header().Rrtype) {
						var answer Answer
						answer.Id = msg.Id
						answer.Time = time.Unix(int64(*message.ResponseTimeSec), int64(*message.ResponseTimeNsec))
						answer.Name = rr.Header().Name
						answer.Ttl = rr.Header().Ttl
						answer.Class = rr.Header().Class
						answer.Type = rr.Header().Rrtype
						if data, ok := data(rr); ok {
							answer.Data = data
						} else {
							fmt.Fprintf(os.Stderr, "Failed to get response (answer) data from RR \"%s\"\n", rr.String())
							continue
						}

						answers = append(answers, answer)
					}
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "dns.Msg.Unpack(...) failed: %s\n", e)
		}
	}

	return answers, e
}

type resolverResponseJsonMessageHandler struct {
	output io.Writer
}

func resolverResponseMessage(message *dnstap.Message) bool {
	return message != nil && *message.Type == dnstap.Message_RESOLVER_RESPONSE && message.ResponseMessage != nil
}

func (this *resolverResponseJsonMessageHandler) Handle(message *dnstap.Message) {
	if resolverResponseMessage(message) {
		if answers, e := answers(message, []uint16{}); e == nil {
			for _, answer := range answers {
				if json, ok := answer.Json(); ok {
					this.output.Write([]byte(json + "\n"))
				}
			}
		}
	}
}

func (this *resolverResponseJsonMessageHandler) Close() {
	//
}

func NewResolverResponseJsonMessageHandler(output io.Writer) dnstapserver.DnstapMessageHandler {
	return &resolverResponseJsonMessageHandler{output: output}
}

type resolverResponseSqliteMessageHandler struct {
	db      *sql.DB
	size    int
	answers []Answer
}

func (this *resolverResponseSqliteMessageHandler) insert(answers []Answer) error {
	if 0 < len(answers) {
		if insert, e := this.db.Prepare("INSERT INTO answers (time, id, name, ttl , class, type, data) VALUES(?, ?, ?, ?, ?, ?, ?)"); e == nil {
			if transaction, e := this.db.Begin(); e == nil {
				for _, answer := range answers {
					_, e := transaction.Stmt(insert).Exec(answer.Time.Format("2006-01-02 15:04:05.999999"), answer.Id, strings.TrimRight(answer.Name, "."), answer.Ttl, dns.ClassToString[answer.Class], dns.TypeToString[answer.Type], strings.TrimRight(answer.Data, "."))
					if e != nil {
						if sqle, ok := e.(sqlite3.Error); ok {
							if sqle.ExtendedCode != sqlite3.ErrConstraintPrimaryKey && sqle.ExtendedCode != sqlite3.ErrConstraintUnique {
								transaction.Rollback()
								return e
							}
						} else {
							transaction.Rollback()
							return e
						}
					}
				}
				transaction.Commit()
			} else {
				fatalln(e)
			}
		} else {
			fatalln(e)
		}
	}
	return nil
}

func (this *resolverResponseSqliteMessageHandler) Handle(message *dnstap.Message) {
	if resolverResponseMessage(message) {
		// add the answsers to the cache
		if answers, e := answers(message, []uint16{dns.TypeA, dns.TypeAAAA, dns.TypeCNAME}); e == nil && 0 < len(answers) {
			this.answers = append(this.answers, answers...)
		}

		// insert cached answers to the database
		if this.size < len(this.answers) {
			for attempt := 0; attempt < 30; attempt++ {
				if e := this.insert(this.answers); e == nil {
					this.answers = this.answers[:0] //([]Answer, 0)
					return
				} else {
					fmt.Fprintln(os.Stderr, e)
					time.Sleep(2 * time.Second)
				}
			}
			fatalln("Failed to insert message into database")
		}
	}
}

func (this *resolverResponseSqliteMessageHandler) Close() {
	if 0 < len(this.answers) {
		for attempt := 0; attempt < 8; attempt++ {
			if e := this.insert(this.answers); e == nil {
				this.answers = this.answers[:0]
				return
			} else {
				fmt.Fprintln(os.Stderr, e)
			}
		}
		fatalln("Failed to insert message into database")
	}
}

func NewResolverResponseSqliteMessageHandler(database string, cache int) dnstapserver.DnstapMessageHandler {
	fmt.Fprintf(os.Stdout, "Creating SQLite3 message handler \"%v\"\n", database)

	db, e := sql.Open("sqlite3", database)
	if e != nil {
		fatalln(e)
	}

	if _, e := db.Exec(`CREATE TABLE IF NOT EXISTS answers (
		time TEXT NOT NULL,
		id INTEGER NOT NULL,
		name TEXT NOT NULL,
		ttl INTEGER NOT NULL,
		class TEXT NOT NULL,
		type TEXT NOT NULL,
		data TEXT NOT NULL,
		UNIQUE (time, id, name, ttl, class, type, data)
	);`); e != nil {
		fatalln(e)
	}

	if _, e := db.Exec(`CREATE INDEX IF NOT EXISTS idx_answers_name ON answers(name);`); e != nil {
		fatalln(e)
	}

	if _, e := db.Exec(`CREATE INDEX IF NOT EXISTS idx_answers_data ON answers(data);`); e != nil {
		fatalln(e)
	}

	return &resolverResponseSqliteMessageHandler{db: db, size: cache, answers: make([]Answer, 0)}
}
