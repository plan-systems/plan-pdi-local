package datastore

import (
	//"os"
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/plan-systems/plan-core/pdi"
	"github.com/plan-systems/plan-core/plan"
	"github.com/plan-systems/plan-core/ski"

	"github.com/plan-systems/plan-core/ski/Providers/hive"
)

var gTestBuf = "May PLAN empower organizations and individuals, and may it be an instrument of productivity and self-organization."

var gTesting *testing.T

var gCommunityEpoch = pdi.CommunityEpoch{
	CommunityID:   []byte{0, 1, 2, 3, 4, 5, 5, 7, 99, 123},
	CommunityName: "encoding-test",
	EntryHashKit:  ski.HashKitID_Blake2b_256,
}

func TestVarAppendBuf(t *testing.T) {

	gTesting = t

	testBufs := make([][]byte, 100)

	for i := range testBufs {
		N := int(1 + rand.Int31n(int32(len(gTestBuf))-2))
		testBufs[i] = []byte(gTestBuf[:N])
	}

	buf := make([]byte, len(testBufs)*len(gTestBuf))
	totalLen := 0

	var err error
	for i := range testBufs {
		totalLen, err = pdi.AppendVarBuf(buf, totalLen, testBufs[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	buf = buf[:totalLen]

	offset := 0
	var payload []byte

	for i := range testBufs {
		offset, payload, err = pdi.ReadVarBuf(buf, offset)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(payload, testBufs[i]) {
			t.Fatalf("'%v' != '%v'", string(payload), string(testBufs[i]))
		}
	}

	if offset != len(buf) {
		t.Fatalf("expected offset == %v, got %v'", len(buf), offset)
	}
}

func TestTxnEncoding(t *testing.T) {

	gTesting = t

	session, err := hive.StartSession("", "test", nil)
	if err != nil {
		gTesting.Fatal(err)
	}

	tool, err := ski.NewSessionTool(
		session,
		"Charles",
		gCommunityEpoch.CommunityID,
	)
	if err != nil {
		gTesting.Fatal(err)
	}

	stEpoch, err := NewStorageEpoch(tool.Session, &gCommunityEpoch)
	if err != nil {
		gTesting.Fatal(err)
	}
	stEpoch.TxnMaxSize = 10000

	{
		authorKey := ski.KeyRef{
			KeyringName: stEpoch.StorageKeyringName(),
		}

		err := tool.GetLatestKey(&authorKey, ski.KeyType_SigningKey)
		if err != nil {
			gTesting.Fatal(err)
		}
	}

	A := &testSession{
		*tool,
		nil,
		nil,
	}

	// Register providers to test
	encodersToTest := []func() (pdi.TxnEncoder, pdi.TxnDecoder){
		func() (pdi.TxnEncoder, pdi.TxnDecoder) {
			decoder := NewTxnDecoder(true)
			encoder := NewTxnEncoder(false, *stEpoch)
			return encoder, decoder
		},
	}

	for _, createCoders := range encodersToTest {

		A.encoder, A.decoder = createCoders()

		txnEncodingTest(A, stEpoch)

		A.EndSession("done A")
	}
}

func txnEncodingTest(A *testSession, stEpoch *pdi.StorageEpoch) {

	seed := plan.Now()
	//seed = int64(1559235863)
	gTesting.Logf("using seed %d", seed)
	rand.Seed(seed)

	totalBytes := 0
	totalTxns := 0
	totalPayloads := 0

	// Test agent encode/decode
	{

		blobBuf := make([]byte, 500000)
		decoder := A.decoder
		encoder := A.encoder

		err := encoder.ResetSigner(A.Session, nil)
		if err != nil {
			gTesting.Fatal(err)
		}

		var (
			swizzle               []int
			payloadIn, payloadOut pdi.RawTxn
			txnSet                *pdi.PayloadTxnSet
		)

		collator := pdi.NewTxnCollater()

		for j := 0; j < 5000; j++ {
			testTime := plan.Now()

			payloadLen := int(1 + rand.Int31n(int32(stEpoch.TxnMaxSize)*25))

			payloadIn.Bytes = blobBuf[:payloadLen]
			rand.Read(payloadIn.Bytes)

			totalBytes += payloadLen
			totalPayloads++

			payloadPb, err := payloadIn.Marshal()

			payloadTxns, err := encoder.EncodeToTxns(
				payloadPb,
				plan.Encoding_Unspecified,
				nil,
				testTime+int64(j),
			)
			if err != nil {
				gTesting.Fatal(err)
			}

			N := len(payloadTxns.Segs)
			totalTxns += N

			// Setup a shuffle the txn segment order
			{
				if cap(swizzle) < N {
					swizzle = make([]int, N, N+100)
				} else {
					swizzle = swizzle[:N]
				}
				for i := 0; i < N; i++ {
					swizzle[i] = i
				}
				if true {
					rand.Shuffle(N, func(i, j int) {
						swizzle[i], swizzle[j] = swizzle[j], swizzle[i]
					})
				}
			}

			gTesting.Logf("#%d: Testing %d segment txn set (payloadSz=%d)", j, N, len(payloadIn.Bytes))

			for i := 0; i < N; i++ {
				idx := swizzle[i]
				gTesting.Logf("Decoding %d (%d) of %d", i, idx, N)
				txnSet, err = collator.DecodeAndCollateTxn(decoder, &pdi.RawTxn{
					Bytes: payloadTxns.Segs[idx].RawTxn,
					URID:  payloadTxns.Segs[idx].Info.URID,
				})
				if i < N-1 {
					plan.Assert(txnSet == nil, "txnSet should be nil")
				} else {
					plan.Assert(txnSet != nil, "txnSet should be non nil")
				}
				if err != nil {
					gTesting.Fatal(err)
				}
			}

			err = txnSet.UnmarshalPayload(&payloadOut)
			if err != nil {
				gTesting.Fatal(err)
			}

			pdi.RecycleTxnSet(txnSet)

			if !bytes.Equal(payloadIn.Bytes, payloadOut.Bytes) {
				gTesting.Fatal("payload check failed")
			}
		}
	}

	fmt.Printf("payloads: %d    bytes: %d   txns: %d\n", totalPayloads, totalBytes, totalTxns)
}

type testSession struct {
	ski.SessionTool

	encoder pdi.TxnEncoder
	decoder pdi.TxnDecoder
}
