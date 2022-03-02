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
	"errors"
	"fmt"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/sys"
)

type matchFn func(device *sys.Device, drive *directcsi.DirectCSIDrive) (match bool, consider bool, err error)

var (
	errNoMatchFound = errors.New("no matching drive found")
)

var matchers = []matchFn{
	// v1beta3 matchers
	// HWInfoMatchers (conclusive)
	partitionNumberMatcher,
	ueventSerialNumberMatcher,
	wwidMatcher,
	modelNumberMatcher,
	vendorMatcher,
	// If there are more than one matching drive, continue.. else conclude
	// SWInfoMatchers (non conclusive)
	//partitionTableUUIDMatcher,
	partitionUUIDMatcher,
	dmUUIDMatcher,
	mdUUIDMatcher,
	//filesystemMatcher,
	ueventFSUUIDMatcher,

	// // If there are more than one matching drive, continue.. else conclude
	// // v1beta2 conclusive matchers
	fsUUIDMatcher,
	serialNumberMatcher,

	// // If there are more than one matching drive, continue.. else conclude
	// // v1beta1 conclusive matchers
	logicalBlocksizeMatcher,
	// physicalBlocksizeMatcher,
	// totalCapacityMatcher,
	// allocatedCapacityMatcher,
}

func runMatcher(device *sys.Device, drives []*directcsi.DirectCSIDrive) (*directcsi.DirectCSIDrive, error) {
	matchedDrives, _ := getMatchingDrives(device, drives)
	switch len(matchedDrives) {
	case 1:
		return matchedDrives[0], nil
	case 0:
		return nil, errNoMatchFound
	default:
		// handle too many matches
		//
		// case 1: It is possible to have an empty/partial drive (&directCSIDrive.Status{Path: /dev/sdb0, "", "", "", ...})
		//         to match  with a correct match

		// case 2: A terminating drive and an actual drive can be matched with the single device
		//
		// case 3: A duplicate drive (due to any bug)
		//
		// ToDo: make these drives invalid / decide based on drive status / calculate ranks and decide
		return nil, fmt.Errorf("device %s has too many matches", device.Name)
	}
}

func getMatchingDrives(device *sys.Device, drives []*directcsi.DirectCSIDrive) (matchedDrives []*directcsi.DirectCSIDrive, matchList map[*directcsi.DirectCSIDrive][]matchFn) {
	for _, drive := range drives {
		var matchedFn []matchFn
		for _, matchFn := range matchers {
			match, cont, _ := matchFn(device, drive)
			if cont {
				continue
			}
			if match {
				matchedDrives = append(matchedDrives, drive)
			}
			matchedFn = append(matchedFn, matchFn)
			break
		}
		matchList[drive] = matchedFn
	}
	return
}

func isDOSPTType(ptType string) bool {
	switch ptType {
	case "dos", "msdos", "mbr":
		return true
	default:
		return false
	}
}

func partitionNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return drive.Status.PartitionNum == device.Partition, false, nil
}

func ueventSerialNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.UeventSerial, drive.Status.UeventSerial)
}

func wwidMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.WWID, drive.Status.WWID)
}

func modelNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.Model, drive.Status.ModelNumber)
}

func vendorMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.Vendor, drive.Status.Vendor)
}

func partitionUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.PartUUID, drive.Status.PartitionUUID)
}

func dmUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.DMUUID, drive.Status.DMUUID)
}

func mdUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.MDUUID, drive.Status.MDUUID)
}

func serialNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.Serial, drive.Status.SerialNumber)
}

func serialNumberLongMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.SerialLong, drive.Status.SerialNumberLong)
}

func pciPathMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return immutablePropertyMatcher(device.PCIPath, drive.Status.PCIPath)
}

// Refer https://go.dev/play/p/zuaURPArfcL
// ###################################### Truth table Hardware Matcher ####################################
//	| alpha | beta |				Match 						|			Not-Match 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   0	|   0  |	match=false, consider=true, err = nil   | 			XXXXXXX 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   0	|   1  |				XXXXXXX     	            | match=false, consider=false, err = nil  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   0  |	match=false, consider=true, err = nil   | 			XXXXXXX 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   1  |	match=true, consider=false, err = nil   | match=false, consider=false, err = nil  |
//  |-------|------|--------------------------------------------|-----------------------------------------|

func immutablePropertyMatcher(alpha string, beta string) (bool, bool, error) {
	var match, consider bool
	var err error
	switch {
	case alpha == "" && beta == "":
		consider = true
	case alpha == "" && beta != "":
	case alpha != "" && beta == "":
		consider = true
	case alpha != "" && beta != "":
		if alpha == beta {
			match = true
		}
	}
	return match, consider, err

}

func ueventFSUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return fsPropertyMatcher(device.UeventFSUUID, drive.Status.UeventFSUUID)
}

func fsTypeMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return fsPropertyMatcher(device.FSType, drive.Status.Filesystem)
}

func fsUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return fsPropertyMatcher(device.FSUUID, drive.Status.FilesystemUUID)
}

// Refer https://go.dev/play/p/zuaURPArfcL
// ###################################### Truth table Hardware Matcher ####################################
//	| alpha | beta |				Match 						|			Not-Match 					  |
//	|-------|------|--------------------------------------------|------------------------------------------
// 	|   0	|   0  |	match=false, consider=true, err = nil   | 			XXXXXXX 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   0	|   1  |	match=false, consider=true, err = nil   | 			XXXXXXX 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   0  |	match=false, consider=true, err = nil   | 			XXXXXXX 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   1  |	match=true, consider=false, err = nil   | match=false, consider=true , err = nil  |
//  |-------|------|--------------------------------------------|------------------------------------------

func fsPropertyMatcher(alpha string, beta string) (bool, bool, error) {
	var match, consider bool
	var err error
	switch {
	case alpha == "" && beta == "":
		consider = true
	case alpha == "" && beta != "":
		consider = true
	case alpha != "" && beta == "":
		consider = true
	case alpha != "" && beta != "":
		if alpha == beta {
			match = true
		} else {
			consider = true
		}
	}
	return match, consider, err
}

func logicalBlocksizeMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return sizeMatcher(int64(device.LogicalBlockSize), drive.Status.LogicalBlockSize)
}

func totalCapacityMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return sizeMatcher(int64(device.TotalCapacity), drive.Status.TotalCapacity)
}

func sizeMatcher(alpha int64, beta int64) (bool, bool, error) {
	var match, consider bool
	var err error
	switch {
	case alpha == 0 && beta == 0:
		consider = true
	case alpha == 0 && beta != 0:
		consider = true
	case alpha != 0 && beta == 0:
		consider = true
	case alpha != 0 && beta != 0:
		if alpha == beta {
			match = true
		} else {
			consider = true
		}
	}
	return match, consider, err
}

// Refer https://go.dev/play/p/zuaURPArfcL
// ###################################### Truth table Hardware Matcher ####################################
//	| alpha | beta |				Match 						|			Not-Match 					  |
//	|-------|------|--------------------------------------------|------------------------------------------
// 	|   0	|   0  |	match=false, consider=true, err = nil   | 			XXXXXXX 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   0	|   1  |				XXXXXXX           			| match=false, consider=true, err = nil   |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   0  |				XXXXXXX 					| match=false, consider=true, err = nil   |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   1  |	match=true, consider=false, err = nil   | match=false, consider=fals , err = nil  |
//  |-------|------|--------------------------------------------|-----------------------------------------|

func physicalBlocksizeMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	var match, consider bool
	var err error
	switch {
	case int64(device.PhysicalBlockSize) == 0 && drive.Status.PhysicalBlockSize == 0:
		consider = true
	case int64(device.PhysicalBlockSize) == 0 && drive.Status.PhysicalBlockSize != 0:
		consider = true
	case int64(device.PhysicalBlockSize) != 0 && drive.Status.PhysicalBlockSize == 0:
		consider = true
	case int64(device.PhysicalBlockSize) != 0 && drive.Status.PhysicalBlockSize != 0:
		if int64(device.PhysicalBlockSize) == drive.Status.PhysicalBlockSize {
			match = true
		}
	}
	return match, consider, err
}

// Refer https://go.dev/play/p/zuaURPArfcL
// ###################################### Truth table Hardware Matcher ####################################
//	| alpha | beta |				Match 						|			Not-Match 					  |
//	|-------|------|--------------------------------------------|------------------------------------------
// 	|   0	|   0  |	match=false, consider=true, err = nil   | 			XXXXXXX 					  |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   0	|   1  |				XXXXXXX           			| match=false, consider=true, err = nil   |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   0  |				XXXXXXX 					| match=false, consider=true, err = nil   |
//	|-------|------|--------------------------------------------|-----------------------------------------|
// 	|   1	|   1  |	match=true, consider=false, err = nil   | match=false, consider=true , err = nil  |
//  |-------|------|--------------------------------------------|-----------------------------------------|

// func partitionTableUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
// 	if drive.Status.PartTableUUID == "" || drive.Status.PartTableType == "" {
// 		return false, true, nil
// 	}
// 	return drive.Status.PartTableUUID == device.PTUUID &&
// 		ptTypeEqual(drive.Status.PartTableType, device.PTType), false, nil
// }

// func logicalBlocksizeMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
// 	return drive.Status.LogicalBlockSize == int64(device.LogicalBlockSize), false, nil
// }

// func ptTypeEqual(ptType1, ptType2 string) bool {
// 	ptType1, ptType2 = strings.ToLower(ptType1), strings.ToLower(ptType2)
// 	switch {
// 	case ptType1 == ptType2:
// 		return true
// 	case isDOSPTType(ptType1) && isDOSPTType(ptType2):
// 		return true
// 	default:
// 		return false
// 	}
// }
