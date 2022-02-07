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
	"strconv"
	"strings"
	"syscall"

	"github.com/minio/directpv/pkg/sys"
	"github.com/minio/directpv/pkg/udev"
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
	errNonDeviceEvent = errors.New("Uevent is not for a block device")
	pageSize          = os.Getpagesize()
	fieldDelimiter    = []byte{0}

	errEmptyBuf = errors.New("buffer is empty")
)

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
			eventData, err := listener.getNextDeviceUEvent(ctx)
			if err != nil {
				return err
			}
			listener.queue.Push(eventData)
		}
	}
}

func (l *listener) getNextDeviceUEvent(ctx context.Context) (*udev.Data, error) {
	for {
		buf, err := l.ReadMsg()
		if err != nil {
			return nil, err
		}

		udevData, err := l.parseUdevData(buf)
		if err != nil {
			if errors.Is(err, errNonDeviceEvent) {
				continue
			}
			return nil, err
		}
		return udevData, nil
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

func (l *listener) parseUdevData(buf []byte) (*udev.Data, error) {
	eventMap, err := parse(buf)
	if err != nil {
		return nil, err
	}

	if eventMap["SUBSYSTEM"] == "block" {
		return nil, errNonDeviceEvent
	}

	switch eventMap["ACTION"] {
	case add, change:
		// Older kernels like in CentOS 7 does not send all information about the device,
		// hence read relevant data from /run/udev/data/b<major>:<minor>
		major, err := strconv.Atoi(eventMap["MAJOR"])
		if err != nil {
			return nil, err
		}
		minor, err := strconv.Atoi(eventMap["MINOR"])
		if err != nil {
			return nil, err
		}
		return udev.ReadRunUdevData(major, minor)
	case remove:
		return udev.EventMapToUdevData(eventMap)
	default:
		return nil, fmt.Errorf("invalid device action: %s", action)
	}
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
