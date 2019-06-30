package datastore

import (
	//"bytes"
	"sync"

	"github.com/plan-systems/plan-core/pdi"
	"github.com/plan-systems/plan-core/plan"
	"github.com/plan-systems/plan-core/ski"
)

const (

	// ProtocolDesc appears in StorageEpochs originating from this impl
	ProtocolDesc = "plan/storage/pdi-local/1"
)

// NewStorageEpoch generates a new StorageEpoch, needed when creating a new community.
func NewStorageEpoch(
	skiSession ski.Session,
	inCommunity *pdi.CommunityEpoch,
) (*pdi.StorageEpoch, error) {

	epoch := &pdi.StorageEpoch{
		StorageProtocol: ProtocolDesc,
		CommunityID:     inCommunity.CommunityID,
		TxnHashKit:      ski.HashKitID_Blake2b_256,
		Name:            inCommunity.CommunityName,
		TxnMaxSize:      32000,
		CommunityChIDs:  make([]byte, pdi.CommunityChID_NumChannels*plan.ChIDSz),
	}

	var err error
	epoch.OriginKey, err = ski.GenerateNewKey(
		skiSession,
		inCommunity.FormGenesisKeyringName(),
		ski.KeyInfo{
			KeyType:   ski.KeyType_SigningKey,
			CryptoKit: ski.CryptoKitID_NaCl,
		},
	)
	if err != nil {
		return nil, err
	}

	return epoch, err
}

// dsEncoder implements pdi.TxnEncoder
type dsEncoder struct {
	pdi.TxnEncoder

	StorageEpoch pdi.StorageEpoch
	threadsafe   bool
	mutex        sync.Mutex

	packer ski.PayloadPacker
	from   ski.KeyInfo

	// Used to marshal TxnInfo
	scrap []byte
}

// NewTxnEncoder creates a new StorageProviderAgent for use with a pdi-datastore StorageProvider.
// If inSegmentMaxSz == 0, then a default size is chosen
func NewTxnEncoder(
	inMakeThreadsafe bool,
	inStorageEpoch pdi.StorageEpoch,
) pdi.TxnEncoder {

	enc := &dsEncoder{
		StorageEpoch: inStorageEpoch,
		threadsafe:   inMakeThreadsafe,
		packer:       ski.NewPacker(false),
	}

	return enc
}

// ResetSigner --see TxnEncoder
func (enc *dsEncoder) ResetSigner(
	inSession ski.Session,
	inFrom []byte,
) error {

	return enc.packer.ResetSession(
		inSession,
		ski.KeyRef{
			KeyringName: enc.StorageEpoch.StorageKeyringName(),
			PubKey:      inFrom,
		},
		enc.StorageEpoch.TxnHashKit,
		&enc.from)

}

// EncodeToTxns -- See StorageProviderAgent.EncodeToTxns()
func (enc *dsEncoder) EncodeToTxns(
	inPayload []byte,
	inPayloadEnc plan.Encoding,
	inTransfers []*pdi.Transfer,
	timeSealed int64,
) (*pdi.PayloadTxnSet, error) {

	txnSet, err := pdi.SegmentIntoTxns(
		inPayload,
		inPayloadEnc,
		enc.StorageEpoch.TxnMaxSize,
	)
	if err != nil {
		return nil, err
	}

	// Put the transfers in the last segment
	txnSet.Info().Transfers = inTransfers

	// Use the same time stamp for the entire batch
	if timeSealed == 0 {
		timeSealed = plan.Now()
	}

	{
		// We have a mutex b/c of the shared scrap
		if enc.threadsafe {
			enc.mutex.Lock()
		}

		scrap := enc.scrap

		for i, seg := range txnSet.Segs {

			// Set the rest of the txn fields
			seg.Info.TimeSealed = timeSealed
			if i > 0 {
				seg.Info.PrevURID = txnSet.Segs[i-1].Info.URID
			}

			headerSz := seg.Info.Size()
			if headerSz > len(scrap) {
				enc.scrap = make([]byte, headerSz+5000)
				scrap = enc.scrap
			}
			headerSz, err = seg.Info.MarshalTo(scrap)
			if err != nil {
				return nil, plan.Error(nil, plan.MarshalFailed, "failed to marshal txn info")
			}

			packingInfo := ski.PackingInfo{}
			err = enc.packer.PackAndSign(
				plan.Encoding_TxnPayloadSegment,
				scrap[:headerSz],
				seg.PayloadSeg,
				pdi.URIDSz,
				&packingInfo,
			)
			if err != nil {
				return nil, err
			}

			seg.RawTxn = packingInfo.SignedBuf
			seg.Info.URID = pdi.URIDFromTimeAndHash(packingInfo.Extra, seg.Info.TimeSealed, packingInfo.Hash)
		}

		if enc.threadsafe {
			enc.mutex.Unlock()
		}
	}

	return txnSet, nil
}

// dsDecoder implements pdi.TxnDecoder
type dsDecoder struct {
	pdi.TxnDecoder

	unpacker ski.PayloadUnpacker
}

// NewTxnDecoder creates a TxnDecoder for use with pdi-datastore
func NewTxnDecoder(
	inMakeThreadsafe bool,
) pdi.TxnDecoder {

	return &dsDecoder{
		unpacker: ski.NewUnpacker(inMakeThreadsafe),
	}
}

// DecodeRawTxn -- See TxnDecoder
func (dec *dsDecoder) DecodeRawTxn(
	inRawTxn []byte,
	outInfo *pdi.TxnInfo,
) ([]byte, error) {

	out := ski.SignedPayload{
		Hash: make([]byte, 128),
	}

	err := dec.unpacker.UnpackAndVerify(inRawTxn, &out)
	if err != nil {
		return nil, err
	}

	var txnInfo pdi.TxnInfo
	if err := txnInfo.Unmarshal(out.Header); err != nil {
		return nil, plan.Error(err, plan.UnmarshalFailed, "failed to unmarshal TxnInfo")
	}

	txnInfo.From = out.Signer.PubKey
	txnInfo.URID = pdi.URIDFromTimeAndHash(out.Hash[len(out.Hash):], txnInfo.TimeSealed, out.Hash)

	// 2) Isolate the payload buf
	if txnInfo.SegSz != uint32(len(out.Body)) {
		return nil, plan.Errorf(nil, plan.UnmarshalFailed, "txn payload buf length doesn't match")
	}

	if outInfo != nil {
		*outInfo = txnInfo
	}

	return out.Body, nil
}
