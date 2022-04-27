package main

import (
	"github.com/gorilla/websocket"
	"net/http"
	"log"
	"flag"
	"encoding/json"
	"fmt"
	"database/sql"
	"time"
	"github.com/satori/go.uuid"
	"github.com/go-ini/ini"
	"os"
	"github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	"context"
	"runtime"
	"sync"
)

var manager = ClientManager{

	register:   make(chan *WebsocketUser),
	unregister: make(chan *WebsocketUser),
	clients:    make(map[string]*WebsocketUser),
	replyAll:   make(chan []byte),

}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r* http.Request)bool {
		return true
	},


}

//var tpl = template.Must(template.ParseFiles("test.html"))
var mu sync.Mutex
var db * sql.DB
var ws = buildAndInit()
var port = getPort()
var dashboard = ws.dbname
var addr = flag.String("addr", "0.0.0.0:"+ port, "http service address")


const (
	//tempo concesso per scrivere un messaggio al peer.
	writeWait = 10 * time.Second

	//tempo concesso per leggere il prossimo pong msg dal peer.
	pongWait = 180 * time.Second

	//manda pings al peer con questo periodo; deve essere minore del pongWait.
	pingPeriod = (pongWait * 9) / 10
)


func main() {

	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	InitDB()

	defer db.Close()
	ctx, _ := context.WithCancel(context.Background())




	go manager.start()
	http.HandleFunc("/server", handleConnections)
	//http.HandleFunc("/", home)
	go startRedis(ctx,ws.redisAddress)
    // go countGo()


	log.Printf("serving on port " + port + "")
	log.Fatal(http.ListenAndServe(*addr, nil))



}

/*
func home(w http.ResponseWriter, r *http.Request) {
	tpl.Execute(w, "ws://"+r.Host+"/server")
}
*/


// funzione di inizializ. con parsing dei file ini e creazione della mappa dei widgets.

func buildAndInit() *WebSocketServer {

	envFileContent, err := ini.Load("../conf/environment.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}
	genFileContent, err := ini.Load("../conf/general.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}
	dbFileContent, err := ini.Load("../conf/database.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}
	wsServerContent, err := ini.Load("../conf/webSocketServer.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}

	ssoFileContent, err := ini.Load("../conf/sso.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}
	redisServerContent, err := ini.Load("../conf/redis.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}

	wss := new(WebSocketServer)

	a := envFileContent.Sections()
	wss.activeEnv = a[0].Key("environment[value]").String()

	a = genFileContent.Sections()
	wss.host = a[0].Key("host[" + wss.activeEnv + "]").String()

	a = dbFileContent.Sections()
	wss.username = a[0].Key("username[" + wss.activeEnv + "]").String()
	wss.password = a[0].Key("password[" + wss.activeEnv + "]").String()
	wss.dbname = a[0].Key("dbname[" + wss.activeEnv + "]").String()

	a = wsServerContent.Sections()
	wss.serverAddress = a[0].Key("wsServerAddress[" + wss.activeEnv + "]").String()
	wss.serverPort = a[0].Key("wsServerPort[" + wss.activeEnv + "]").String()

	a = ssoFileContent.Sections()
	wss.clientSecret = a[0].Key("ssoClientSecret[" + wss.activeEnv + "]").String()
	wss.clientID = a[0].Key("ssoClientId[" + wss.activeEnv + "]").String()

	a = redisServerContent.Sections()
	wss.redisAddress = a[0].Key("redisHost[" + wss.activeEnv + "]").String()
	wss.redisPassword = a[0].Key("redisPwd[" + wss.activeEnv + "]").String()

	wss.clientWidgets = make( map[string][]*WebsocketUser)

	return wss

}

// goroutine che si occupa dei subscribes/unsubscribes e dell'inoltro delle risposte.

func (manager *ClientManager) start() {
	for {
		select {
		case conn := <-manager.register:
			manager.clients[conn.id] = conn
			log.Println("A new socket has connected")

		case conn := <-manager.unregister:
			if _, ok := manager.clients[conn.id]; ok {
				close(conn.send)
				delete(manager.clients, conn.id)

				closed(conn)
				log.Println("A socket has disconnected")
			}
		case message := <-manager.replyAll:
			for conn := range ws.clientWidgets {

				dat := processingMsg2(message)
				if conn == dat["metricName"]{


					for key := range  ws.clientWidgets[conn]{
						log.Print(conn)
						log.Print(ws.clientWidgets[conn])
						select {

						case ws.clientWidgets[conn][key].send <- message:

							/*default:
								close(ws.clientWidgets[conn][key].send)
						        closed(ws.clientWidgets[conn][key])
						        delete(ws.clientWidgets, ws.clientWidgets[conn][key].id)

*/


						}

					}
				}


			}
		}
	}
}

// handler che fà l'upgrade a websocket, reindirizza i client alla registrazione e lancia le routine di read e write.

func handleConnections(w http.ResponseWriter, r *http.Request) {
	conn, err := (&upgrader).Upgrade(w, r, nil)
	if err != nil {

		log.Print("upgrade:", err)
		return

	}
	client := &WebsocketUser{id: uuid.Must(uuid.NewV4()).String(), socket: conn, send: make(chan []byte)}
	manager.register <- client

	go client.reader()
	go client.writer()

}

// funzione di chiusura connessione che provvede a fare l'unset dei parametri.

func closed(u *WebsocketUser) {
	var unsetKey int
	op := false
	if u.userType == "widgetInstance"{
		log.Print(len(ws.clientWidgets[u.metricName]))
		mu.Lock()
		if len(ws.clientWidgets[u.metricName])==1 {
			publish([]byte("unsubscribe"+u.metricName), "default")
		}
		mu.Unlock()
	}

	if u.widgetUniqueName != nil && u.widgetUniqueName != "" {
		mu.Lock()

		widgets := ws.clientWidgets
		for value := range widgets[u.metricName] {
			if widgets[u.metricName][value] == u{
				unsetKey = value
				op = true
				break
			}

		}
		if op {
			widgets[u.metricName][unsetKey] = widgets[u.metricName][len(widgets[u.metricName])-1]
			widgets[u.metricName][len(widgets[u.metricName])-1] = nil
			widgets[u.metricName] = widgets[u.metricName][:len(widgets[u.metricName])-1]
			ws.clientWidgets = widgets

		}
		mu.Unlock()
	}

}

// routine di read per la lettura dei messaggi. Viene lanciato dbComunication per il processing e la logica sui messaggi.

func (c *WebsocketUser) reader() {
	defer func() {
		manager.unregister <- c
		c.socket.Close()
	}()
	c.socket.SetReadDeadline(time.Now().Add(pongWait))
	c.socket.SetPongHandler(func(string) error { c.socket.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	for {
		_, message, err := c.socket.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}
		log.Printf("recv: %s", message)

		dbCommunication(message, c)
	}

}

//routine di write che raccoglie i messaggi di risposta e invia verso i destinatari.

func (c *WebsocketUser) writer() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		c.socket.Close()
		ticker.Stop()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.socket.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.socket.WriteMessage(websocket.TextMessage, message)

		case <-ticker.C:
			c.socket.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.socket.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// funzione fittizia che uso per testare  via via le query

func dbTest() {

	db, err := sql.Open("mysql", "root:emanuele@tcp(127.0.0.1:3306)/dashboard")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Connection Established")
	}
	defer db.Close()
	var count2 int
	dash := db.QueryRow("SELECT COUNT(*) FROM Dashboard.Config_dashboard "+
		"WHERE title_header = ? AND user = ?;", "asg", "disit")
	err3 := dash.Scan(&count2)
	if err3 != nil {
		panic(err.Error())
	}
	fmt.Println(count2)

	myString := `{"msgType" : "Firenze", "metricName" : 3, "newValue" : {"personNumber": 321, "lat": 6.05}}`
	mapstr, newVal := processingMsg([]byte(myString))
	fmt.Println(mapstr)
	fmt.Println(newVal)

}

//funzioni per la decodifica dei messaggi json; sono usate in dbProcessing.

func processingMsg2(jsonMsg []byte) map[string]interface{} {
	var dat map[string]interface{}
	err := json.Unmarshal(jsonMsg, &dat)
	if err != nil {
		log.Println("Decoding error:", err)
	}
	return dat
}

func processingMsg(jsonMsg []byte) (map[string]interface{}, map[string]interface{}) {
	var dat map[string]interface{}
	err := json.Unmarshal(jsonMsg, &dat)
	if err != nil {
		log.Println("Decoding error:", err)

	}
	parse, err := json.Marshal(dat["newValue"])

	//dat["newValue"] = fmt.Sprint(string(parse))
	var jsonParsed map[string]interface{}
	json.Unmarshal(parse, &jsonParsed)

	return dat, jsonParsed

}

// apro la connessione al database

func InitDB(){

	config := mysql.NewConfig()

	config.User = ws.username
	config.Passwd = ws.password
	config.Addr = ws.serverAddress
	config.DBName = ws.dbname
	confstring := config.FormatDSN()

	var err error
	db, err = sql.Open("mysql", confstring)
	if err != nil{
		log.Panic(err)
	}
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(60*time.Second)

	//db.SetMaxOpenConns(qualche quantita` da definire)

}

// funzione lanciata dal main tramite goroutine per far partire il listener sul canale Pub/Sub.

func startRedis(ctx context.Context, redisServerAddr string){
	err := listenPubSubChannels(ctx, redisServerAddr,
		func() error {

			// la callback di start e` un buon posto per implementare il riempimento
			// delle notifiche perse. Per adesso, non fa essenzialmente nulla.


			return nil
		},
		func(channel string, message []byte) error {
			fmt.Printf("channel: %s, message: %s\n", channel, message)

			manager.replyAll <- message
			return nil
		},
		"newData")

	if err != nil {
		log.Print(err)
		
	}

}

// listenPubSubChannels ascolta i messaggi nel canale Redis di Pub/Sub. la funzione
// onStart e` chiamata subito dopo la sottoscrizione ai canali. la funzione onMessage
// e` invece chiamata dopo ogni messaggio.

func listenPubSubChannels(ctx context.Context, redisServerAddr string,
	onStart func() error,
	onMessage func(channel string, data []byte) error,
	channels ...string) error {

	// Un ping viene settato con questo periodo per controllare l'integrita`
	// della connessione e del server.

	const healthCheckPeriod = time.Minute

	c, err := redis.Dial("tcp", redisServerAddr,
		// il read timeout sul server dovrebbe essere piu` grande del periodo di ping.
		redis.DialReadTimeout(healthCheckPeriod+10*time.Second),
		redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		return err
	}
	defer c.Close()
	c.Do("AUTH", ws.redisPassword)

	psc := redis.PubSubConn{Conn: c}

	if err := psc.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return err
	}

	done := make(chan error, 1)

	// Lancia una goroutine per ricevere notifiche dal server.
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case error:
				done <- n
				return
			case redis.Message:
				data := string(n.Data)
				if len(data) >= 9{
					if data[0:9] == "subscribe"{
						psc.Subscribe(data[9:len(data)])
						break
					}

					if data[0:11] == "unsubscribe"{
						psc.Unsubscribe(data[11:len(data)])
						break
					}
				}
				if err := onMessage(n.Channel, n.Data); err != nil {
					done <- err
					return
				}
			case redis.Subscription:
				switch n.Count {
				case len(channels):
					// notifica l'applicazione quando tutti i canali sono sottoscritti.
					if err := onStart(); err != nil {
						done <- err
						return
					}
				case 0:
					// ritorna dalla goroutine quando tutti i canali sono disiscritti.
					done <- nil
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()
loop:
	for err == nil {
		select {
		case <-ticker.C:

			// Manda il ping per testare la salute della connessione e del server.
			// se il corrispondente pong non e' ricevuto, allora la ricezione sulla
			// connessione andra` in timeout e la corrispondente goroutine ritornera`.

			if err = psc.Ping(""); err != nil {
				break loop
			}
		case <-ctx.Done():
			break loop
		case err := <-done:
			// ritorna errore dalla goroutine di ricezione.
			log.Print(err)
			return err
		}
	}

	// Segnala alla goroutine di ricezione di uscire disiscrivendo dai canali .
	psc.Unsubscribe()

	// Aspetta il completamento della goroutine.
	return <-done
}

// funzione per il publish su redis.

func publish(msg []byte, channel string) {
	if channel == "default"{
		channel = "newData"
	}
	c, err := redis.Dial("tcp", ws.redisAddress)
	if err != nil {
		log.Print(err)
		return
	}
	defer c.Close()
	c.Do("AUTH", ws.redisPassword)

	c.Do("PUBLISH", channel, msg)


}

func countGo(){
	for {
		time.Sleep(5 * time.Second)
		log.Print(runtime.NumGoroutine())
		log.Print(manager.clients)

	}
}



func getPort() string{
	if len(os.Args) > 1{

		return os.Args[1]

	}else{

		return ws.serverPort
	}

}