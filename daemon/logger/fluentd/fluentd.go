package fluentd

import (
	"io"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	"github.com/fluent/fluent-logger-golang/fluent"
)

type Fluentd struct {
	tag           string
	containerid   string
	containername string
	writer        *fluent.Fluent
}

const name = "fluentd"

func init() {
	if err := logger.RegisterLogDriver(name, New); err != nil {
		logrus.Fatal(err)
	}
}

func New(ctx logger.Context) (logger.Logger, error) {
	config := ctx.Config
	port := 24224
	host := "localhost"
	if config["port"] != "" {
		port, _ = strconv.Atoi(config["port"])
	}
	if config["host"] != "" {
		host = config["host"]
	}
	tag := config["tag"]
	if tag == "" {
		tagPrefix := "docker."
		if config["tag-prefix"] != "" {
			tagPrefix = config["tag-prefix"] + "."
		}
		if config["tag-type"] == "name" {
			tag = tagPrefix + ctx.ContainerName
		} else if config["tag-type"] == "fullid" {
			tag = tagPrefix + ctx.ContainerID
		} else { // id
			tag = tagPrefix + ctx.ContainerID[:12]
		}
	}
	log, err := fluent.New(fluent.Config{FluentPort: port, FluentHost: host})
	logrus.Debugf("logging driver fluentd configured for container:%s.", ctx.ContainerID)
	logrus.Debugf("logging driver fluentd port:%d.", port)
	logrus.Debugf("logging driver fluentd host:%s.", host)
	logrus.Debugf("logging driver fluentd tag:%s.", tag)
	if err != nil {
		return nil, err
	}
	return &Fluentd{
		tag:           tag,
		containerid:   ctx.ContainerID,
		containername: ctx.ContainerName,
		writer:        log,
	}, nil
}

func (f *Fluentd) Log(msg *logger.Message) error {
	var data = map[string]string{
		"container_id":   f.containerid,
		"container_name": f.containername,
		"source":         msg.Source,
		"log":            string(msg.Line),
	}
	f.writer.PostWithTime(f.tag, msg.Timestamp, data)
	// fluent-logger-golang buffers logs from failures and disconnections,
	// and these are transferred again automatically.
	return nil
}

func (f *Fluentd) Close() error {
	return f.writer.Close()
}

func (f *Fluentd) Name() string {
	return name
}

func (s *Fluentd) GetReader() (io.Reader, error) {
	return nil, logger.ReadLogsNotSupported
}
