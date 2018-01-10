package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/robfig/cron"
	"github.com/tidwall/buntdb"
	"golang.org/x/crypto/ssh"
	"gopkg.in/olahol/melody.v1"
)

func getSSHSession(user, password, host string) *ssh.Session {

	NumberOfPrompts := 3

	// Normally this would be a callback that prompts the user to answer the
	// provided questions
	Cb := func(user, instruction string, questions []string, echos []bool) (answers []string, err error) {
		log.Printf("user is %v, instruction is %v, questions is %v, echos is %v", user, instruction, questions, echos)
		return []string{user, password}, nil
	}

	config := &ssh.ClientConfig{
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		User:            user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
			ssh.RetryableAuthMethod(ssh.KeyboardInteractiveChallenge(Cb), NumberOfPrompts),
		},
	}
	client, err := ssh.Dial("tcp", host, config)
	if err != nil {
		log.Printf("unable to : %v", err)
	}
	session, err := client.NewSession()
	if err != nil {
		log.Printf("err is %s", err)
	}
	return session
}

func openShell(session *ssh.Session) {
	modes := ssh.TerminalModes{
		ssh.ECHO:          0,     // disable echoing
		ssh.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
		ssh.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
	}
	// Request pseudo terminal
	if err := session.RequestPty("xterm", 40, 80, modes); err != nil {
		log.Fatal("request for pseudo terminal failed: ", err)
	}
	// Start remote shell
	if err := session.Shell(); err != nil {
		log.Fatal("failed to start shell: ", err)
	}
}

type TaskCron struct {
	Schedule cron.Schedule
	LastTime time.Time
}

func main() {
	// To serve a directory on disk (/tmp) under an alternate URL
	// path (/tmpfiles/), use StripPrefix to modify the request
	// URL's path before the FileServer sees it:
	// http.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir("."))))
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
	timer := cron.New()
	timer.Start()
	taskMap := make(map[string]*TaskCron)
	timer.AddFunc("* * * * * *", func() {
		for key, value := range taskMap {
			now := time.Now()
			next := value.Schedule.Next(value.LastTime)
			if now.After(next) {
				value.LastTime = now
				//log.Printf("-----------------------task run, now is %s, next is %s", now, next)
				go http.Get(key)
			}

		}
	})
	db, err := buntdb.Open("task.aof")
	if err != nil {
		log.Fatal(err)
	}
	db.Shrink()

	db.View(func(tx *buntdb.Tx) error {
		err := tx.AscendKeys("task:*", func(key, value string) bool {
			sendURL := strings.Replace(key, "task:", "", 1)
			sendCron := value
			log.Printf("load from db, url is %s, cron is %s", sendURL, sendCron)

			sche, err := cron.Parse(sendCron)
			if err != nil {
				return true
			}
			taskMap[sendURL] = &TaskCron{Schedule: sche, LastTime: time.Unix(0, 0)}
			return true
		})
		return err
	})

	ws := melody.New()
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir(".")))
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.HandleRequest(w, r)
	})
	mux.HandleFunc("/rest/timer", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		sendURL := r.Form.Get("url")
		sendCron := r.Form.Get("cron")
		sche, err := cron.Parse(sendCron)
		if err != nil {
			return
		}

		db.Update(func(tx *buntdb.Tx) error {
			tx.Set(fmt.Sprintf("task:%s", sendURL), sendCron, nil)
			return nil
		})
		taskMap[sendURL] = &TaskCron{Schedule: sche, LastTime: time.Unix(0, 0)}
		log.Printf("now is %s, next run time is %s", time.Now(), sche.Next(time.Now()))

	})

	mux.HandleFunc("/rest/tasktest1", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("+++++++++++++++++++tasktest1 get %s", time.Now())
	})

	mux.HandleFunc("/rest/tasktest2", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("+++++++++++++++++++tasktest2 get %s", time.Now())
	})

	// shellOutput := make(chan []byte)
	session := getSSHSession("yxsicd", "root", "localhost:22")
	openShell(session)

	var in, out, serr bytes.Buffer
	session.Stdout = &out
	session.Stdin = &in
	session.Stderr = &serr

	ws.HandleMessage(func(wssession *melody.Session, message []byte) {
		in.Write(message)
		ws.Broadcast(message)
	})

	go func() {
		for {
			buf := make([]byte, 3*1024)
			ret, _ := out.Read(buf)
			if ret > 0 {
				ws.Broadcast(buf)
			}
		}
	}()

	log.Fatal(http.ListenAndServe(":8080", mux))
}
