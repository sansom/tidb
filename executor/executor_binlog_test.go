// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"net"
	"os"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type mockBinlogPump struct {
	payloads [][]byte
}

func (p *mockBinlogPump) WriteBinlog(ctx context.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	p.payloads = append(p.payloads, req.Payload)
	return &binlog.WriteBinlogResp{}, nil
}

func (s *testSuite) TestBinlog(c *C) {
	unixFile := "/tmp/mock-binlog-pump"
	defer os.Remove(unixFile)
	l, err := net.Listen("unix", unixFile)
	c.Assert(err, IsNil)
	defer l.Close()
	serv := grpc.NewServer()
	pump := new(mockBinlogPump)
	binlog.RegisterPumpServer(serv, pump)
	go serv.Serve(l)
	opt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	})
	clientCon, err := grpc.Dial(unixFile, opt, grpc.WithInsecure())
	c.Assert(err, IsNil)
	c.Assert(clientCon, NotNil)
	binloginfo.PumpClient = binlog.NewPumpClient(clientCon)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists local_binlog")
	tk.MustExec("create table local_binlog (id int primary key, name varchar(10))")

	tk.MustExec("insert local_binlog values (1, 'abc'), (2, 'cde')")
	time.Sleep(time.Millisecond)
	prewriteVal := getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.SchemaVersion, Greater, int64(0))
	c.Assert(prewriteVal.Mutations[0].TableId, Greater, int64(0))
	insertedRow1, _ := codec.EncodeValue(nil, types.NewIntDatum(1), types.NewStringDatum("abc"))
	insertedRow2, _ := codec.EncodeValue(nil, types.NewIntDatum(2), types.NewStringDatum("cde"))
	c.Assert(prewriteVal.Mutations[0].InsertedRows, DeepEquals, [][]byte{insertedRow1, insertedRow2})

	tk.MustExec("update local_binlog set name = 'xyz' where id = 2")
	time.Sleep(time.Millisecond)
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	updatedRow, _ := codec.EncodeValue(nil, types.NewIntDatum(2), types.NewStringDatum("xyz"))
	c.Assert(prewriteVal.Mutations[0].UpdatedRows, DeepEquals, [][]byte{updatedRow})

	tk.MustExec("delete from local_binlog where id = 1")
	time.Sleep(time.Millisecond)
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].DeletedIds, DeepEquals, []int64{1})

	// Test table primary key is not integer.
	tk.MustExec("create table local_binlog2 (name varchar(64) primary key)")
	tk.MustExec("insert local_binlog2 values ('abc'), ('def')")
	time.Sleep(time.Millisecond)
	tk.MustExec("delete from local_binlog2 where name = 'def'")
	time.Sleep(time.Millisecond)
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	_, deletedPK, _ := codec.DecodeOne(prewriteVal.Mutations[0].DeletedPks[0])
	c.Assert(deletedPK.GetString(), Equals, "def")

	// Test Table don't have primary key.
	tk.MustExec("create table local_binlog3 (c1 int, c2 int)")
	tk.MustExec("insert local_binlog3 values (1, 2), (1, 3), (2, 3)")
	time.Sleep(time.Millisecond)
	tk.MustExec("delete from local_binlog3 where c2 = 3")
	time.Sleep(time.Millisecond)
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)

	deletedRow1, _ := codec.Decode(prewriteVal.Mutations[0].DeletedRows[0], 2)
	c.Assert(deletedRow1[1], DeepEquals, types.NewIntDatum(3))
	deletedRow2, _ := codec.Decode(prewriteVal.Mutations[0].DeletedRows[1], 2)
	c.Assert(deletedRow2[1], DeepEquals, types.NewIntDatum(3))

	binloginfo.PumpClient = nil
}

func getLatestBinlogPrewriteValue(c *C, pump *mockBinlogPump) *binlog.PrewriteValue {
	idx := len(pump.payloads) - 2
	c.Assert(idx, Greater, 0)
	prewritePayload := pump.payloads[idx]
	commitPayload := pump.payloads[idx+1]
	bin := &binlog.Binlog{}
	bin.Unmarshal(commitPayload)
	c.Assert(bin.Tp, Equals, binlog.BinlogType_Commit)
	c.Assert(bin.StartTs, Greater, int64(0))
	c.Assert(bin.CommitTs, Greater, int64(0))
	c.Assert(bin.PrewriteKey, NotNil)
	bin = &binlog.Binlog{}
	err := bin.Unmarshal(prewritePayload)
	c.Assert(err, IsNil)
	c.Assert(bin.Tp, Equals, binlog.BinlogType_Prewrite)
	c.Assert(bin.StartTs, Greater, int64(0))
	c.Assert(bin.PrewriteKey, NotNil)
	c.Assert(bin.PrewriteValue, NotNil)
	prewriteValue := new(binlog.PrewriteValue)
	err = prewriteValue.Unmarshal(bin.PrewriteValue)
	c.Assert(err, IsNil)
	return prewriteValue
}
