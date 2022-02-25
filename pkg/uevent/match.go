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
	"strings"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/sys"
)

func isDOSPTType(ptType string) bool {
	switch ptType {
	case "dos", "msdos", "mbr":
		return true
	default:
		return false
	}
}

func ptTypeEqual(ptType1, ptType2 string) bool {
	ptType1, ptType2 = strings.ToLower(ptType1), strings.ToLower(ptType2)
	switch {
	case ptType1 == ptType2:
		return true
	case isDOSPTType(ptType1) && isDOSPTType(ptType2):
		return true
	default:
		return false
	}
}

func matchWWID(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.WWID == "" || drive.Status.WWID == device.WWID
	return matched, matched
}

func matchUeventSerial(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.UeventSerial == "" || drive.Status.UeventSerial == device.UeventSerial
	return matched, matched
}

func matchDMUUID(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.DMUUID == "" || drive.Status.DMUUID == device.DMUUID
	return matched, matched
}

func matchMDUUID(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.MDUUID == "" || drive.Status.MDUUID == device.MDUUID
	return matched, matched
}

func matchPTUUID(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.PartTableUUID == "" || drive.Status.PartTableUUID == device.PTUUID ||
		drive.Status.PartTableType == "" || ptTypeEqual(drive.Status.PartTableType, device.PTType)
	return matched, matched
}

func matchPartUUID(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.PartitionUUID == "" || drive.Status.PartitionUUID == device.PartUUID
	return matched, matched
}
