package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var logger TransactionLogger

func initializeTransactionLogger() error {
	var err error

	logger, err = NewFileTransactionLogger("transactions.log")
	if err != nil {
		return fmt.Errorf("failed to initialize transaction logger: %w", err)
	}

	events, errors := logger.ReadEvents()

	e := Event{}
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errors: //retrieve any errors
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello gorilla/mux!")
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer r.Body.Close()

	logger.WritePut(key, string(value))
	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusCreated)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, value)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	logger.WriteDelete(key)

	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func main() {

	err := initializeTransactionLogger()
	if err != nil {
		log.Fatalf("Failed to initialize transaction logger: %v", err)
		return
	}

	r := mux.NewRouter()

	r.HandleFunc("/", helloMuxHandler)
	r.HandleFunc("/v1/key/{key}", putHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", getHandler).Methods("GET")
	r.HandleFunc("/v1/key/{key}", deleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8085", r))
}
