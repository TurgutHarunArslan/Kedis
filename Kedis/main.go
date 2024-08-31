package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

type Write struct {
	Key   string
	Value string
}

var 
(
	data = make(map[string]string)
	mu sync.Mutex
	writeChannel = make(chan Write, 1000)
)

func HandleCommand(message string) string {
	parts := strings.Split(message, " ")

	if len(parts) == 0 {
		return "ERROR: No command provided"
	}

	command := strings.ToUpper(parts[0])
	switch command {
	case "SET":

		if len(parts) < 3 {
			return "ERROR: Not enough arguments for SET command"
		}
		mu.Lock()
		defer mu.Unlock()
		data[parts[1]] = parts[2]

		writeChannel <- Write{Key: parts[1],Value: parts[2]}
		return "OK"

	case "GET":
		if len(parts) < 2 {
			return "ERROR: Not enough arguments for GET command"
		}
		key := parts[1]
		mu.Lock()
		defer mu.Unlock()
		value, exists := data[key]
		if exists == false {
			return "ERROR: Key not found"
		}
		return value
	default:
		return "ERROR: Unknown command"
	}
}

func HandleCon(c net.Conn) {
	defer c.Close()
	reader := bufio.NewReader(c)
	for {
		message, err := reader.ReadString('\n')

		if err != nil {
			return
		}

		message = strings.TrimSpace(message)
		response := HandleCommand(message)
		_, err = c.Write([]byte(response + "\n"))
		if err != nil {
			log.Println("Error writing:", err)
			return
		}
	}
}

func WriteDbWorker(){
	file, err := os.OpenFile("data.rdb", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println("Error opening RDB file:", err)
		return
	}
	for cmd := range writeChannel{
		_, err := file.WriteString(fmt.Sprintf("%s:%s\n", cmd.Key, cmd.Value))
		if err != nil {
			log.Println("Error writing to RDB file:", err)
		}
	}
	defer file.Close()
}

func StartServer() {
	l, err := net.Listen("tcp", ":2000")
	log.Println("Started tcp server at :2000")
	if err != nil {
		log.Fatal(err)
	}

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalln(err)
			continue
		}

		go HandleCon(conn)
	}

}

func LoadSnapshot() {
	file, err := os.Open("data.rdb")
	if err != nil {
		log.Println("Error opening RDB file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			log.Println("Error parsing line:", line)
			continue
		}

		key := parts[0]
		value := parts[1]
		mu.Lock()
		data[key] = value
		mu.Unlock()
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error reading from RDB file:", err)
	}
}

func main() {
	LoadSnapshot()
	go WriteDbWorker()
	StartServer()
}
