package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"github.com/stianeikeland/go-rpio"
)

// コマンドラインフラグの初期値
const (
	defHost     = "test.mosquitto.org"
	defPort     = "1883"
	defClientID = "mqtt-rp-pir-pub"
	defUsername = ""
	defPassword = ""
	defTopic    = "mqtt-rp-pir"
	defQoS      = 0
)

// MQTTブローカ切断時の待ち時間(ms)
const quiesce = 1000

const (
	// 割り込みシグナルのチェック間隔(ms)
	checkInterval = 1000
	// モーション検知のチェック間隔(ms)
	detectInterval = 1000
)

// PIRセンサーのGPIOピン番号
const pinPIR = 7

type msg struct {
	topic   string
	content string
	qos     mqtt.QoS
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

	// MQTTブローカサーバへの接続
	chMsg, chConQuit, err := connect(cli)
	if err != nil {
		panic(err)
	}

	// モーション検知の開始
	chDetQuit, err := detect(chMsg, *topic, mqtt.QoS(*qos))
	if err != nil {
		// 処理の終了をPublish goroutineへ通知
		chConQuit <- struct{}{}
		<-chConQuit

		panic(err)
	}

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

	// 処理の終了を各goroutineへ通知
	chConQuit <- struct{}{}
	chDetQuit <- struct{}{}

	// 各goroutineから処理終了の完了通知を受信
	<-chConQuit
	<-chDetQuit
}

// connectはMQTTブローカサーバへ接続する。
func connect(cli *mqtt.MqttClient) (chan<- msg, chan struct{}, error) {
	// ブローカサーバへの接続
	log.Println("ブローカサーバへ接続しています...")

	if _, err := cli.Start(); err != nil {
		return nil, nil, err
	}

	log.Println("ブローカサーバへ接続しました。")

	// channelの作成
	chMsg := make(chan msg, 1024)
	chConQuit := make(chan struct{})

	// Publishを実施するgoroutineの起動
	go func() {
		defer func() {
			log.Println("ブローカサーバから切断しています...")
			cli.Disconnect(quiesce)
			log.Println("ブローカサーバから切断しました。")
			chConQuit <- struct{}{}
		}()

	PubLoop:
		for {
			select {
			case m := <-chMsg:
				// Publishの実施
				log.Println("Publishを実施しています...")
				receipt := cli.Publish(m.qos, m.topic, m.content)
				log.Println("Publishを実施しました。")
				// Publishの実施完了を待つ
				<-receipt
			case <-chConQuit:
				break PubLoop
			}
		}
	}()

	return chMsg, chConQuit, nil
}

// detectはモーション検知を実施する。
func detect(chMsg chan<- msg, topic string, qos mqtt.QoS) (chan struct{}, error) {
	if err := rpio.Open(); err != nil {
		return nil, err
	}

	pin := rpio.Pin(pinPIR)
	pin.Input()

	log.Println("モーション検知の準備が完了しました。")

	// channelの作成
	chDetQuit := make(chan struct{})

	// モーション検知を実施するgoroutineの起動
	go func() {
		defer func() {
			rpio.Close()
			chDetQuit <- struct{}{}
		}()

	DetLoop:
		for {
			if pin.Read() == rpio.High {
				log.Println("モーションを検知しました。")
				chMsg <- msg{
					topic:   topic,
					content: time.Now().String() + " モーションを検知しました。",
					qos:     qos,
				}
			}

			select {
			case <-chDetQuit:
				break DetLoop
			default:
				time.Sleep(detectInterval * time.Millisecond)
			}
		}
	}()

	return chDetQuit, nil
}
