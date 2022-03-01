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
	"testing"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/sys"
)

func TestPartitionNumberMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartitionNum: 0}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartitionNum: 1}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartitionNum: 2}}
	case1Device := &sys.Device{Partition: 0}
	case2Device := &sys.Device{Partition: 1}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, true, false, nil},
		{case1Device, &case2Drive, false, false, nil},
		{case1Device, &case3Drive, false, false, nil},
		{case2Device, &case2Drive, true, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := partitionNumberMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestUeventSerialNumberMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{UeventSerial: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{UeventSerial: "serial"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{UeventSerial: "serial123"}}
	case1Device := &sys.Device{UeventSerial: "serial"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := ueventSerialNumberMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestWWIDMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{WWID: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{WWID: "wwid"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{WWID: "wwid123"}}
	case1Device := &sys.Device{WWID: "wwid"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := wwidMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestModelNumberMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{ModelNumber: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{ModelNumber: "KXG6AZNV512G TOSHIBA"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{ModelNumber: "KXG6AZ DELL"}}
	case1Device := &sys.Device{Model: "KXG6AZNV512G TOSHIBA"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := modelNumberMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestVendorMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{Vendor: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{Vendor: "TOSHIBA"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{Vendor: "DELL"}}
	case1Device := &sys.Device{Vendor: "TOSHIBA"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := vendorMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestPartitionTableUUIDMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartTableUUID: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartTableType: ""}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartTableUUID: "ptuuid", PartTableType: "pttype"}}
	case4Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartTableUUID: "ptuuid123", PartTableType: "pttype"}}
	case1Device := &sys.Device{PTUUID: "ptuuid", PTType: "pttype"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, false, true, nil},
		{case1Device, &case3Drive, true, false, nil},
		{case1Device, &case4Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := partitionTableUUIDMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestPartitionUUIDMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartitionUUID: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartitionUUID: "part-uuid"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{PartitionUUID: "part-uuid123"}}
	case1Device := &sys.Device{PartUUID: "part-uuid"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := partitionUUIDMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestDMUUIDMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{DMUUID: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{DMUUID: "dm-uuid"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{DMUUID: "dm-uuid123"}}
	case1Device := &sys.Device{DMUUID: "dm-uuid"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := dmUUIDMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestMDUUIDMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{MDUUID: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{MDUUID: "mduuid"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{MDUUID: "mduuid123"}}
	case1Device := &sys.Device{MDUUID: "mduuid"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := mdUUIDMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestUeventFSUUIDMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{UeventFSUUID: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{UeventFSUUID: "ueventfsuuid"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{UeventFSUUID: "different-ueventfsuuid"}}
	case1Device := &sys.Device{UeventFSUUID: "ueventfsuuid"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := ueventFSUUIDMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestFSUUIDMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{FilesystemUUID: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{FilesystemUUID: "fsuuid"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{FilesystemUUID: "different-fsuuid"}}
	case1Device := &sys.Device{FSUUID: "fsuuid"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := fsUUIDMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestSerialNumberMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{SerialNumber: ""}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{SerialNumber: "KXG6AZNV512G TOSHIBA_31IF73XDFDM3"}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{SerialNumber: "different-KXG6AZNV512G TOSHIBA_31IF73XDFDM3"}}
	case1Device := &sys.Device{Serial: "KXG6AZNV512G TOSHIBA_31IF73XDFDM3"}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, false, true, nil},
		{case1Device, &case2Drive, true, false, nil},
		{case1Device, &case3Drive, false, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := serialNumberMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1,
				match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}

func TestLogicalBlocksizeMatcher(t *testing.T) {
	case1Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{LogicalBlockSize: 0}}
	case2Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{LogicalBlockSize: 1024}}
	case3Drive := directcsi.DirectCSIDrive{Status: directcsi.DirectCSIDriveStatus{LogicalBlockSize: 2048}}
	case1Device := &sys.Device{LogicalBlockSize: 0}
	case2Device := &sys.Device{LogicalBlockSize: 1024}
	testCases := []struct {
		device   *sys.Device
		drive    *directcsi.DirectCSIDrive
		match    bool
		consider bool
		err      error
	}{
		{case1Device, &case1Drive, true, false, nil},
		{case1Device, &case2Drive, false, false, nil},
		{case1Device, &case3Drive, false, false, nil},
		{case2Device, &case2Drive, true, false, nil},
	}

	for i, testCase := range testCases {
		match, consider, err := logicalBlocksizeMatcher(testCase.device, testCase.drive)
		if match != testCase.match || consider != testCase.consider || err != testCase.err {
			t.Fatalf("case %v: expected: match %v , consider %v , error %v ; got: match %v  consider %v  error %v ", i+1, match, consider, err, testCase.match, testCase.consider, testCase.err)
		}
	}
}
