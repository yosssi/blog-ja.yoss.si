package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

// コマンドラインフラグの初期値
const (
	defHost     = "test.mosquitto.org"
	defPort     = "1883"
	defClientID = "mosquitto-test-sub"
	defUsername = ""
	defPassword = ""
	defTopic    = "mosquitto-test"
	defQoS      = 0
)

// MQTTブローカ切断時の待ち時間(ms)
const quiesce = 1000

// 割り込みシグナルのチェック間隔(ms)
const checkInterval = 1000

// handleはメッセージ受信時の処理を実施する。
func handle(_ *mqtt.MqttClient, msg mqtt.Message) {
	log.Printf("メッセージを受信しました。\nTopic: %s\nMessage: %s\n", msg.Topic(), msg.Payload())
}

func main() {
	// シグナル通知設定
	chSig := make(chan os.Signal, 1)
	signal.Notify(chSig, os.Interrupt, os.Kill)

	// コマンドラインフラグのパース
	host := flag.String("h", defHost, "MQTTブローカサーバのホスト名")
	port := flag.String("p", defPort, "MQTTブローカサーバのポート番号")
	clientID := flag.String("c", defClientID, "クライアントID")
	username := flag.String("u", defUsername, "認証ユーザ名")
	password := flag.String("pw", defPassword, "認証パスワード")
	topic := flag.String("t", defTopic, "トピック")
	qos := flag.Int("q", defQoS, "QoS")

	flag.Parse()

	// MQTTクライアントの作成
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + *host + ":" + *port)
	opts.SetClientId(*clientID)
	if *username != "" && *password != "" {
		opts.SetUsername(*username)
		opts.SetPassword(*password)
	}

	cli := mqtt.NewClient(opts)

	// ブローカサーバへの接続
	log.Println("ブローカサーバへ接続しています...")
	if _, err := cli.Start(); err != nil {
		panic(err)
	}

	defer func() {
		log.Println("ブローカサーバから切断しています...")
		cli.Disconnect(quiesce)
		log.Println("ブローカサーバから切断しました。")
	}()

	log.Println("ブローカサーバへ接続しました。")

	// フィルタの設定
	filter, err := mqtt.NewTopicFilter(*topic, byte(*qos))
	if err != nil {
		panic(err)
	}

	// Subscriptionの開始
	log.Println("Subscriptionを開始しています...")
	if _, err := cli.StartSubscription(handle, filter); err != nil {
		panic(err)
	}

	defer func() {
		// Subscriptionの終了
		log.Println("Subscriptionを終了しています...")
		receipt, err := cli.EndSubscription(*topic)
		if err != nil {
			panic(err)
		}

		// Subscriptionの終了完了を待つ
		<-receipt
		log.Println("Subscriptionを終了しました。")
	}()

	log.Println("Subscriptionを開始しました。")

	// 割り込み発生まで待つ
WaitLoop:
	for {
		select {
		case <-chSig:
			break WaitLoop
		default:
			time.Sleep(checkInterval * time.Millisecond)
		}
	}
}
