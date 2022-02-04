// This file is part of MinIO DirectPV
// Copyright (c) 2021, 2022 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package uevent

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/minio/directpv/pkg/sys"
)

const (
	libudev      = "libudev\x00"
	libudevMagic = 0xfeedcafe
	minMsgLen    = 40

	add    = "add"
	change = "change"
	remove = "remove"
)

var (
	pageSize       = os.Getpagesize()
	fieldDelimiter = []byte{0}

	errNonDeviceEvent = errors.New("Uevent is not for a block device")
	errEmptyBuf       = errors.New("buffer is empty")
)

type deviceEvent struct {
	Path  string
	Major int
	Minor int
	Action string
	Partition int
	WWID string
	Model string
	UeventSerial string
	Vendor string
	DMName string
	DMUUID string
	MDUUID string
	PTUUID string
	PTType string
	PartUUID string
	UeventFSUUID string
	FSType string
	FSUUID string
}

type listener struct {
	sockfd      int
	queue       *queue
	threadiness int

	handler DeviceUEventHandler
}

type DeviceUEventHandler interface {
	Change(context.Context, *sys.Device) error
	Delete(context.Context, *sys.Device) error
}

func Run(ctx context.Context, handler DeviceUEventHandler) error {
	sockfd, err := syscall.Socket(
		syscall.AF_NETLINK,
		syscall.SOCK_RAW,
		syscall.NETLINK_KOBJECT_UEVENT,
	)
	if err != nil {
		return err
	}

	if err := syscall.Bind(sockfd, &syscall.SockaddrNetlink{
		Family: syscall.AF_NETLINK,
		Pid:    uint32(os.Getpid()),
		Groups: 2,
	}); err != nil {
		return err
	}

	listener := &listener{
		sockfd:  sockfd,
		handler: handler,
		queue:   newQueue,
	}

	go listener.processEvents(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			event, err := listener.getNextDeviceUEvent(ctx)
			if err != nil {
				return err
			}
			listener.queue.Push(event)
		}
	}
}

func (l *Listener) getNextDeviceUEvent(ctx context.Context) (*deviceEvent, error) {
	for {
		buf, err := l.ReadMsg()
		if err != nil {
			return nil, err
		}

		dEv, err := l.unmarshalDeviceUevent(buf)
		if err != nil {
			if errors.Is(errNonBlockDevice) {
				continue
			}
			return nil, err
		}
		return dEv, nil
	}
}

func parse(msg []byte) (map[string]string, error) {
	if !bytes.HasPrefix(msg, []byte(libudev)) {
		return nil, errors.New("libudev signature not found")
	}

	// magic number is stored in network byte order.
	if magic := binary.BigEndian.Uint32(msg[8:]); magic != libudevMagic {
		return nil, fmt.Errorf("libudev magic mismatch; expected: %v, got: %v", libudevMagic, magic)
	}

	offset := int(msg[16])
	if offset < 17 {
		return nil, fmt.Errorf("payload offset %v is not more than 17", offset)
	}
	if offset > len(msg) {
		return nil, fmt.Errorf("payload offset %v beyond message length %v", offset, len(msg))
	}

	fields := bytes.Split(msg[offset:], fieldDelimiter)
	event := map[string]string{}
	for _, field := range fields {
		if len(field) == 0 {
			continue
		}
		switch tokens := strings.SplitN(string(field), "=", 2); len(tokens) {
		case 1:
			event[tokens[0]] = ""
		case 2:
			event[tokens[0]] = tokens[1]
		}
	}
	return event, nil
}

func normalizeUUID(uuid string) string {
	if u := strings.ReplaceAll(strings.ReplaceAll(uuid, ":", ""), "-", ""); len(u) > 20 {
		uuid = fmt.Sprintf("%v-%v-%v-%v-%v", u[:8], u[8:12], u[12:16], u[16:20], u[20:])
	}
	return uuid
}

func (l *listener) unmarshalDeviceUevent(buf []byte) (*deviceEvent, error) {
	eventMap, err := parse(buf)
	if err != nil {
		return nil, err
	}

	if eventMap["SUBSYSTEM"] == "block" {
		return nil, errNonDeviceEvent
	}

	name := filepath.Base(eventMap["DEVPATH"])
	if name == "" {
		return nil, fmt.Errorf("event does not have valid DEVPATH %v", event["DEVPATH"])
	}

	major, err := strconv.Atoi(eventMap["MAJOR"])
	if err != nil {
		return nil, err
	}

	minor, err := strconv.Atoi(eventMap["MINOR"])
	if err != nil {
		return nil, err
	}

	action := eventMap["ACTION"]
	switch action {
	case add, change, delete:
	default:
		return nil, fmt.Errorf("invalid action: %s", action)

	var partition int
	if value, found := eventMap["ID_PART_ENTRY_NUMBER"]; found {
		partition, err = strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
	}

	return &deviceEvent{
		Path: name,
		Major: major,
		Minor: minor,
		Action: action,
		Partition: partition,
		WWID: eventMap["ID_WWN"],
		Model: eventMap["ID_MODEL"],
		UeventSerial: eventMap["ID_SERIAL_SHORT"],
		Vendor: eventMap["ID_VENDOR"],
		DMName: eventMap["DM_NAME"],
		DMUUID: eventMap["DM_UUID"],
		MDUUID: normalizeUUID(eventMap["MD_UUID"]),
		PTUUID: eventMap["ID_PART_TABLE_UUID"],
		PTType: eventMap["ID_PART_TABLE_TYPE"],
		PartUUID: eventMap["ID_PART_ENTRY_UUID"],
		UeventFSUUID: eventMap["ID_FS_UUID"],
		FSType: eventMap["ID_FS_TYPE"],
		FSUUID: eventMap["ID_FS_UUID"],
	}, nil
}

func (l *listener) msgPeek() (int, *[]byte, error) {
	var n int
	var err error
	buf := make([]byte, os.Getpagesize())
	for {
		if n, _, err = syscall.Recvfrom(l.sockfd, buf, syscall.MSG_PEEK); err != nil {
			return n, nil, err
		}

		if n < len(buf) {
			break
		}

		buf = make([]byte, len(buf)+os.Getpagesize())
	}
	return n, &buf, err
}

func (l *listener) msgRead(buf *[]byte) error {
	if buf == nil {
		return errEmptyBuf
	}

	n, _, err := syscall.Recvfrom(l.sockfd, *buf, 0)
	if err != nil {
		return err
	}

	*buf = (*buf)[:n]

	return nil
}

// ReadMsg allow to read an entire uevent msg
func (l *listener) ReadMsg() ([]byte, error) {
	_, buf, err := c.msgPeek()
	if err != nil {
		return nil, err
	}
	if err = c.msgRead(buf); err != nil {
		return nil, err
	}

	return *buf, nil
}
