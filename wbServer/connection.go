package main

import (
	"log"
	"github.com/coreos/go-oidc"
	"fmt"
	"golang.org/x/oauth2"
	"net/http"
	"bytes"
	"encoding/json"
	"context"
	"io/ioutil"
)


type Message struct {

	MsgType    string       `json:"msgType"`
	MetricName string       `json:"metricName"`
	NewValue   interface{}  `json:"newValue"`
}

type WebSocketServer struct {

	activeEnv     string
	host          string
	username      string
	password      string
	dbname        string
	serverAddress string
	serverPort    string
	redisAddress  string
	redisPassword string
	clientSecret  string
	clientID      string
	clientWidgets map[string][]*WebsocketUser
}

type callBody struct {

	ElementId   int64   `json:"elementId"`
	ElementType string  `json:"elementType"`
	ElementName string  `json:"elementName"`
}


type ClientManager struct {
	clients    map[string]*WebsocketUser
	register   chan *WebsocketUser
	unregister chan *WebsocketUser
	replyAll   chan []byte
}

func openIDConnect(newDashId int64, title interface{}, dat map[string]interface{}) string {

	callBody := callBody{ElementId: newDashId, ElementType: "DashboardID", ElementName: title.(string)}
	jsonValue, _ := json.Marshal(callBody)

	ctx := context.TODO()
	_, err := oidc.NewProvider(ctx, "http://"+ ws.host +":"+"8080/auth/realms/master")


	if err != nil {
		log.Print(err)
		fmt.Println("Ownership call failed")
		return "Ko"
	}
	oauth2Config := oauth2.Config{
		ClientID:     ws.clientID,
		ClientSecret: ws.clientSecret,


		Endpoint: oauth2.Endpoint{
			TokenURL: "https://"+ ws.host +"/auth/realms/master/protocol/openid-connect/token"},


		Scopes: []string{oidc.ScopeOpenID},
	}
	_ = oauth2Config

	fmt.Println(dat["accessToken"])

	apiURL := "http://"+ ws.serverAddress +"/ownership-api/v1/register/?accessToken=" + fmt.Sprint(dat["accessToken"])

	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonValue))
	defer resp.Body.Close()
	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))

	if err != nil {
		log.Print(err)
		return "Ko"

	} else {

		return "Ok"
	}
}
