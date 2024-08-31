package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type KedisClient struct{
	conn net.Conn
}

func NewKedisClient(address string) (*KedisClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &KedisClient{conn: conn}, nil
}

func (kc *KedisClient) Set(key string, value string) (bool,error) {
	_, err := fmt.Fprintf(kc.conn, "SET %s %s\n", key, value)
	if err != nil{
		return false,err
	}

	response,err := bufio.NewReader(kc.conn).ReadString('\n')

	if err != nil{
		return false,err
	}

	if response == "OK"{
		return true,nil
	}
	return false,nil
}

func (kc *KedisClient) Get(key string) (string,bool) {
	_, err := fmt.Fprintf(kc.conn, "GET %s\n", key)
	if err != nil{
		return "",false
	}

	response,err := bufio.NewReader(kc.conn).ReadString('\n')

	if err != nil{
		return "",false
	}

	return strings.TrimSpace(response),true
}

func (kc *KedisClient) Close() {
	kc.conn.Close()
}