package fluentd

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"text/template"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	"github.com/fluent/fluent-logger-golang/fluent"
)

type Fluentd struct {
	tag           string
	containerID   string
	containerName string
	writer        *fluent.Fluent
}

type Receiver struct {
	ID     string
	FullID string
	Name   string
}

const name = "fluentd"

const defaultHostName = "localhost"
const defaultPort = 24224
const defaultTagPrefix = "docker"

func init() {
	if err := logger.RegisterLogDriver(name, New); err != nil {
		logrus.Fatal(err)
	}
}

func parseConfig(ctx logger.Context) (string, int, string, error) {
	host := defaultHostName
	port := defaultPort
	tag := "docker." + ctx.ContainerID[:12]

	config := ctx.Config

	if config["fluentd-address"] != "" {
		address := config["fluentd-address"]
		if h, p, err := net.SplitHostPort(address); err != nil {
			if strings.Contains(err.Error(), "missing port in address") {
				host = h
			} else {
				return "", 0, "", err
			}
		} else {
			portnum, e := strconv.Atoi(p)
			if e != nil {
				return "", 0, "", e
			}
			host = h
			port = portnum
		}
	}

	if config["fluentd-tag"] != "" {
		receiver := &Receiver{
			ID:     ctx.ContainerID[:12],
			FullID: ctx.ContainerID,
			Name:   ctx.ContainerName,
		}
		tmpl, err := template.New("tag").Parse(config["fluentd-tag"])
		if err != nil {
			return "", 0, "", err
		}
		buf := new(bytes.Buffer)
		err = tmpl.Execute(buf, receiver)
		if err != nil {
			return "", 0, "", err
		}
		tag = buf.String()
	}

	return host, port, tag, nil
}

func New(ctx logger.Context) (logger.Logger, error) {
	host, port, tag, err := parseConfig(ctx)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("logging driver fluentd configured for container:%s.", ctx.ContainerID)
	logrus.Debugf("logging driver fluentd port:%d.", port)
	logrus.Debugf("logging driver fluentd host:%s.", host)
	logrus.Debugf("logging driver fluentd tag:%s.", tag)

	log, err := fluent.New(fluent.Config{FluentPort: port, FluentHost: host})
	if err != nil {
		return nil, err
	}
	return &Fluentd{
		tag:           tag,
		containerID:   ctx.ContainerID,
		containerName: ctx.ContainerName,
		writer:        log,
	}, nil
}

func (f *Fluentd) Log(msg *logger.Message) error {
	data := map[string]string{
		"container_id":   f.containerID,
		"container_name": f.containerName,
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
