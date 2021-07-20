// This file is part of MinIO Direct CSI
// Copyright (c) 2021 MinIO, Inc.
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

package listener2

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	directcsi "github.com/minio/direct-csi/pkg/apis/direct.csi.min.io/v1beta2"
	"github.com/minio/direct-csi/pkg/clientset"
	clientsetfake "github.com/minio/direct-csi/pkg/clientset/fake"
	"github.com/minio/direct-csi/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	nodeID = "test-node"
	mb20   = int64(20 * 1024 * 1024)
	mb30   = int64(30 * 1024 * 1024)
	mb50   = int64(50 * 1024 * 1024)
	mb100  = int64(100 * 1024 * 1024)
)

type testEventHandler struct {
	t               *testing.T
	kubeClient      kubernetes.Interface
	directCSIClient clientset.Interface
	handleFunc      func(t *testing.T, args EventArgs) error
}

func (handler *testEventHandler) ListerWatcher() cache.ListerWatcher {
	// fieldSelector := fields.Everything()
	// if handler.NodeID() != "" {
	// 	fieldSelector = fields.OneTermEqualSelector(
	// 		utils.NodeLabel, utils.SanitizeLabelV(handler.NodeID()),
	// 	)
	// }
	//
	// return cache.NewListWatchFromClient(
	// 	handler.directCSIClient.DirectV1beta2().RESTClient(),
	// 	"DirectCSIVolumes",
	// 	"",
	// 	fieldSelector,
	// )

	return &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return handler.directCSIClient.DirectV1beta2().DirectCSIVolumes().List(context.TODO(), options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return handler.directCSIClient.DirectV1beta2().DirectCSIVolumes().Watch(context.TODO(), options)
		},
	}
}

func (handler *testEventHandler) RESTClient() rest.Interface {
	return handler.directCSIClient.DirectV1beta2().RESTClient()
}

func (handler *testEventHandler) KubeClient() kubernetes.Interface {
	return handler.kubeClient
}

func (handler *testEventHandler) Name() string {
	return "volume"
}

func (handler *testEventHandler) Resource() string {
	return "DirectCSIVolumes"
}

func (handler *testEventHandler) Namespace() string {
	return ""
}

func (handler *testEventHandler) NodeID() string {
	return nodeID
}

func (handler *testEventHandler) ObjectType() runtime.Object {
	return &directcsi.DirectCSIVolume{}
}

func (handler *testEventHandler) Handle(ctx context.Context, args EventArgs) error {
	handler.handleFunc(handler.t, args)
	return nil
}

func startTestController(t *testing.T, objects []runtime.Object, handleFunc func(t *testing.T, args EventArgs) error) context.CancelFunc {
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}

	listener := NewListener(
		&testEventHandler{
			kubeClient:      kubernetesfake.NewSimpleClientset(),
			directCSIClient: clientsetfake.NewSimpleClientset(objects...),
			t:               t,
			handleFunc:      handleFunc,
		},
		"volume-controller",
		hostname,
		1,
	)

	ctx, cancelFunc := context.WithCancel(context.Background())
	if err := listener.Run(ctx); err != nil {
		t.Fatal(err)
	}

	return cancelFunc
}

func TestListener(t *testing.T) {
	testDriveName := "test_drive"
	testVolumeName20MB := "test_volume_20MB"
	testVolumeName30MB := "test_volume_30MB"

	objects := []runtime.Object{
		&directcsi.DirectCSIDrive{
			TypeMeta: utils.DirectCSIDriveTypeMeta(),
			ObjectMeta: metav1.ObjectMeta{
				Name: testDriveName,
				Finalizers: []string{
					string(directcsi.DirectCSIDriveFinalizerDataProtection),
					directcsi.DirectCSIDriveFinalizerPrefix + testVolumeName20MB,
					directcsi.DirectCSIDriveFinalizerPrefix + testVolumeName30MB,
				},
			},
			Status: directcsi.DirectCSIDriveStatus{
				NodeName:          nodeID,
				DriveStatus:       directcsi.DriveStatusInUse,
				FreeCapacity:      mb50,
				AllocatedCapacity: mb50,
				TotalCapacity:     mb100,
			},
		},
		&directcsi.DirectCSIVolume{
			TypeMeta: utils.DirectCSIVolumeTypeMeta(),
			ObjectMeta: metav1.ObjectMeta{
				Name: testVolumeName20MB,
				Finalizers: []string{
					string(directcsi.DirectCSIVolumeFinalizerPurgeProtection),
				},
			},
			Status: directcsi.DirectCSIVolumeStatus{
				NodeName:      nodeID,
				HostPath:      "hostpath",
				Drive:         testDriveName,
				TotalCapacity: mb20,
				Conditions: []metav1.Condition{
					{
						Type:               string(directcsi.DirectCSIVolumeConditionStaged),
						Status:             metav1.ConditionTrue,
						Message:            "",
						Reason:             string(directcsi.DirectCSIVolumeReasonInUse),
						LastTransitionTime: metav1.Now(),
					},
					{
						Type:               string(directcsi.DirectCSIVolumeConditionPublished),
						Status:             metav1.ConditionFalse,
						Message:            "",
						Reason:             string(directcsi.DirectCSIVolumeReasonNotInUse),
						LastTransitionTime: metav1.Now(),
					},
					{
						Type:               string(directcsi.DirectCSIVolumeConditionReady),
						Status:             metav1.ConditionTrue,
						Message:            "",
						Reason:             string(directcsi.DirectCSIVolumeReasonReady),
						LastTransitionTime: metav1.Now(),
					},
				},
			},
		},
		&directcsi.DirectCSIVolume{
			TypeMeta: utils.DirectCSIVolumeTypeMeta(),
			ObjectMeta: metav1.ObjectMeta{
				Name: testVolumeName30MB,
				Finalizers: []string{
					string(directcsi.DirectCSIVolumeFinalizerPurgeProtection),
				},
			},
			Status: directcsi.DirectCSIVolumeStatus{
				NodeName:      nodeID,
				HostPath:      "hostpath",
				Drive:         testDriveName,
				TotalCapacity: mb30,
				Conditions: []metav1.Condition{
					{
						Type:               string(directcsi.DirectCSIVolumeConditionStaged),
						Status:             metav1.ConditionFalse,
						Message:            "",
						Reason:             string(directcsi.DirectCSIVolumeReasonNotInUse),
						LastTransitionTime: metav1.Now(),
					},
					{
						Type:               string(directcsi.DirectCSIVolumeConditionPublished),
						Status:             metav1.ConditionFalse,
						Message:            "",
						Reason:             string(directcsi.DirectCSIVolumeReasonNotInUse),
						LastTransitionTime: metav1.Now(),
					},
					{
						Type:               string(directcsi.DirectCSIVolumeConditionReady),
						Status:             metav1.ConditionTrue,
						Message:            "",
						Reason:             string(directcsi.DirectCSIVolumeReasonReady),
						LastTransitionTime: metav1.Now(),
					},
				},
			},
		},
	}

	handleFunc := func(t *testing.T, args EventArgs) error {
		fmt.Println(args)
		return nil
	}

	cancelFunc := startTestController(t, objects, handleFunc)
	fmt.Println("started test controller")
	defer cancelFunc()
	time.Sleep(3 * time.Second)
}
