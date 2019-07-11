package main

import (
	//"context"
	//"bufio"
	"flag"
	"os"
    "path"
	"fmt"
	"io/ioutil"
	"encoding/json"
	crand "crypto/rand"

	ds "github.com/plan-systems/plan-pdi-local/datastore"

	"github.com/plan-systems/plan-core/tools"
	"github.com/plan-systems/plan-core/tools/ctx"
	"github.com/plan-systems/plan-core/pdi"
	"github.com/plan-systems/plan-core/plan"
	"github.com/plan-systems/plan-core/repo"
	"github.com/plan-systems/plan-core/ski"
	"github.com/plan-systems/plan-core/ski/Providers/hive"
)

func main() {

	init		:= flag.Bool  ( "init",		 false,				    "Creates <datadir> as a fresh/new datastore")
	dataDir	    := flag.String( "datadir",	  "~/_PLAN_pdi-local",	"Specifies the path for all file access and storage")
	genesisFile := flag.String( "genesis",	  "",				    "Creates a new store using the given community genesis json file" )

	flag.Parse()
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")

	sn, err := NewSnode(*dataDir, *init)
	if err != nil {
		ctx.Fatalf("startup failed: %v", err)
	}

	if *init {
		sn.Info(0, "init successful into ", sn.BasePath)
	} else {  
		if len(*genesisFile) > 0 {

			CG, err := loadGenesisInfo(*genesisFile)
			if err != nil {
				sn.Fatalf("error loading genesis file %s: %v", *genesisFile, err)
			}

			err = CG.CreateNewCommunity(sn)
			if err != nil {
				sn.Fatalf("failed to create datastore %s: %v", CG.GenesisSeed.CommunityEpoch.CommunityName, err)
			}
		}

		if err == nil {
			err := sn.Startup()
			if err != nil {
				sn.Fatalf("failed to startup: %v", err)
			} else {
				sn.AttachInterruptHandler()
				sn.CtxWait()
			}	 
		}
	}
}




func loadGenesisInfo(inPathname string) (*CommunityGenesis, error) {

	params := &GenesisParams{}

	buf, err := ioutil.ReadFile(inPathname)
	if err == nil {
		err = json.Unmarshal(buf, params)
	} else {
		///err = nil
		//params.CommunityName = "yoyo"
	}

	if err != nil {
		return nil, err
	}

	if len(params.CommunityName) < 3 {
		return nil, plan.Error(nil, plan.AssertFailed, "missing valid community name")
	}

	// FUTURE: Community ID can also be a hash result of the genesis params so that a community's lookup name is locked in time.
	needed := plan.CommunityIDSz - len(params.CommunityID)
	if needed < 0 {
		params.CommunityID = params.CommunityID[:plan.CommunityIDSz]
	} else if needed > 0 {
		remain := make([]byte, needed)
		crand.Read(remain)
		params.CommunityID = append(params.CommunityID, remain...)
	}

	genesis := &CommunityGenesis{
		GenesisSeed: repo.GenesisSeed{
			CommunityEpoch:  &pdi.CommunityEpoch{
				CommunityID: params.CommunityID,
				CommunityName: params.CommunityName,
				EntryHashKit: ski.HashKitID_Blake2b_256,
				SigningCryptoKit: ski.CryptoKitID_ED25519,
				MaxMemberClockDelta: 120,
			},
		},
	}

	return genesis, nil
}

// CommunityGenesis is a helper for creating a new community
type CommunityGenesis struct {
	ctx.Logger

	GenesisSeed		 	repo.GenesisSeed
	MemberSeed			repo.MemberSeed
	txnsToCommit		[]pdi.RawTxn
}


// CreateNewCommunity creates a new community.
//
// Pre: CommunityEpoch is already set up
func (CG *CommunityGenesis) CreateNewCommunity(
	sn *Snode,
) error {

	CG.SetLogLabel("genesis")

	CG.MemberSeed = repo.MemberSeed{
		RepoSeed: &repo.RepoSeed{
			Services: []*plan.ServiceInfo{
				&plan.ServiceInfo{
					Addr:	sn.Config.GrpcNetworkAddr,
					Network: sn.Config.GrpcNetworkName,
				},
			},  
		},
		MemberEpoch: &pdi.MemberEpoch{
			MemberID: plan.GenesisMemberID, // TODO: randomize?  What ensures Proof of Independence Assurance?
			Alias: "GENESIS",
		},
	}

	genesisSKI, err := hive.StartSession("", "", nil);
	if err != nil { return err }

	// Generate a new storage epoch
	if err == nil {
		CG.GenesisSeed.StorageEpoch, err = ds.NewStorageEpoch(
			genesisSKI,
			CG.GenesisSeed.CommunityEpoch,
		)
		
		// Generate random channel IDs for the community's global/shared channels
		crand.Read(CG.GenesisSeed.StorageEpoch.CommunityChIDs)
	}


   // Generate the first community key  :)
	if err == nil {
		CG.GenesisSeed.CommunityEpoch.KeyInfo, err = ski.GenerateNewKey(
			genesisSKI,
			CG.GenesisSeed.CommunityEpoch.CommunityKeyringName(),
			ski.KeyInfo{
				KeyType: ski.KeyType_SymmetricKey,
				CryptoKit: ski.CryptoKitID_NaCl,
			},
		)
	}  

	// Generate new member private keys
	if err == nil {
		err = CG.MemberSeed.MemberEpoch.RegenMemberKeys(genesisSKI, CG.GenesisSeed.CommunityEpoch)
	}

	// Generate the genesis storage addr
	var genesisAddr []byte
	if err == nil {
		genesisAddr, err = CG.GenesisSeed.StorageEpoch.GenerateNewAddr(genesisSKI)
	}

	pass, err := promptForPass()
	if err != nil { return err }

	// Keep track of what keyrings we need to export, son.
	{
		exportGuide := ski.KeyTome{}
		exportGuide.Keyrings = append(exportGuide.Keyrings, 
			&ski.Keyring{
				Name: CG.GenesisSeed.CommunityEpoch.FormGenesisKeyringName(),
			},
			&ski.Keyring{
				Name: CG.GenesisSeed.CommunityEpoch.CommunityKeyringName(),
			},
			&ski.Keyring{
				Name: CG.MemberSeed.MemberEpoch.FormSigningKeyringName(CG.GenesisSeed.CommunityEpoch.CommunityID),
			},
			&ski.Keyring{
				Name: CG.MemberSeed.MemberEpoch.FormSendingKeyringName(CG.GenesisSeed.CommunityEpoch.CommunityID),
			},
			&ski.Keyring{
				Name: CG.GenesisSeed.StorageEpoch.StorageKeyringName(),
			},
		)
		var opOut *ski.CryptOpOut
		opOut, err = genesisSKI.DoCryptOp(&ski.CryptOpArgs{
			CryptOp: ski.CryptOp_EXPORT_USING_PW,
			PeerKey: []byte(pass),
			TomeIn: &exportGuide,
		})
		if err != nil { 
			return err
		}

		CG.MemberSeed.KeyTome = opOut.BufOut
	}


	// Emit all the genesis entries
	if err == nil {
		crypto := &pdi.MemberCrypto{
			CommunityEpoch: *CG.GenesisSeed.CommunityEpoch,
			StorageEpoch:   *CG.GenesisSeed.StorageEpoch,
			TxnEncoder: ds.NewTxnEncoder(false, *CG.GenesisSeed.StorageEpoch),
			MemberEpoch: *CG.MemberSeed.MemberEpoch,
		}

		err = crypto.StartSession(genesisSKI)
		if err == nil {
			err = CG.emitGenesisEntries(crypto)
		}
		crypto.EndSession("genesis complete")
	}


	if err == nil {
		deposits := []*pdi.Transfer{
			&pdi.Transfer{
				To: genesisAddr,
				Kb:  1 << 40,
				Ops: 1 << 40,
			},
		}

		err = sn.CreateNewStore(
			"badger", 
			deposits,
			CG.txnsToCommit,
			*CG.GenesisSeed.StorageEpoch,
		)
	}

	// Write out the MemberSeed file
	if err == nil {

		packer := ski.NewPacker(false)
		err = packer.ResetSession(
			genesisSKI,
			ski.KeyRef{
				KeyringName: CG.GenesisSeed.CommunityEpoch.FormGenesisKeyringName(),
			}, 
			CG.GenesisSeed.CommunityEpoch.EntryHashKit,
			nil,
		)

		buf, _ := CG.GenesisSeed.Marshal()

		// Pack and sign the genesis seed
		if err == nil { 

			var packingInfo ski.PackingInfo
			err = packer.PackAndSign(0, buf, nil, 0, &packingInfo)

			CG.MemberSeed.RepoSeed.SignedGenesisSeed = packingInfo.SignedBuf
			CG.MemberSeed.RepoSeed.SuggestedDirName = CG.GenesisSeed.FormSuggestedDirName()
			CG.MemberSeed.RepoSeed.CommunityName = CG.GenesisSeed.CommunityEpoch.CommunityName

			// Write out the final MemberSeed file woohoo
			if err == nil { 
				buf, err = CG.MemberSeed.Marshal()

				// TODO: encrypt this and put keys in it 

                outDir, _ := os.Getwd()
                seedFilename := path.Join(outDir, CG.GenesisSeed.CommunityEpoch.CommunityName + ".seed.plan")
                CG.Info(0, "writing genesis seed at ", seedFilename)
				err = ioutil.WriteFile(seedFilename, buf, plan.DefaultFileMode)
			}
		}
	}

	return err
}


type chEntry struct {
	Info			pdi.EntryInfo
	Body			[]byte

	whitelist		bool	
	chEpoch			*pdi.ChannelEpoch
	body			tools.Marshaller
	assignTo		pdi.CommunityChID
	parentEntry		*chEntry
}


func (CG *CommunityGenesis) emitGenesisEntries(mc *pdi.MemberCrypto) error {

	genesisID := uint32(CG.MemberSeed.MemberEpoch.MemberID)

	newACC := &chEntry{
		whitelist: true,
		assignTo: pdi.CommunityChID_RootACC,
		chEpoch: &pdi.ChannelEpoch{
			ChProtocol: repo.ChProtocolACC,
			DefaultAccessLevel: pdi.AccessLevel_READ_ACCESS,
			AccessLevels: map[uint32]pdi.AccessLevel{
				genesisID: pdi.AccessLevel_ADMIN_ACCESS,
			},
		},
	}

	newMemberReg := &chEntry{
		whitelist: true,
		assignTo: pdi.CommunityChID_MemberRegistry,
		chEpoch: &pdi.ChannelEpoch{
			ChProtocol: repo.ChProtocolMemberRegistry,
			ACC: CG.GenesisSeed.StorageEpoch.CommunityChID(pdi.CommunityChID_RootACC),
		},
	}

	newEpochHistory := &chEntry{
		whitelist: true,
		assignTo: pdi.CommunityChID_EpochHistory,
		chEpoch: &pdi.ChannelEpoch{
			ChProtocol: repo.ChProtocolCommunityEpochs,
			ACC: CG.GenesisSeed.StorageEpoch.CommunityChID(pdi.CommunityChID_RootACC),
		},
	}

	postMember := &chEntry{
		whitelist: true,
		body: CG.MemberSeed.MemberEpoch,
		parentEntry: newMemberReg,
		Info: pdi.EntryInfo{
			ChannelID: CG.GenesisSeed.StorageEpoch.CommunityChID(pdi.CommunityChID_MemberRegistry),
		},
	}

	newCommunityHome := &chEntry{
		whitelist: true,
		chEpoch: &pdi.ChannelEpoch{
			ChProtocol: repo.ChProtocolLinks,
			ACC: CG.GenesisSeed.StorageEpoch.CommunityChID(pdi.CommunityChID_RootACC),
		},
	}

	// Do this last so it contains all TIDs resulting from the above
	postGenesisEpoch := &chEntry{
		whitelist: true,
		body: CG.GenesisSeed.CommunityEpoch,
		parentEntry: newEpochHistory,
		Info: pdi.EntryInfo{
			ChannelID: CG.GenesisSeed.StorageEpoch.CommunityChID(pdi.CommunityChID_EpochHistory),
		},
	}

	// We do the post CommunityEpoch first so that the entry ID genrated (now the community epoch ID), can be used for subsequent entries
	entries := []*chEntry{
		newACC,
		newMemberReg,
		newEpochHistory,
		postMember,
		newCommunityHome,
		postGenesisEpoch,
	}

	nowFS := plan.NowFS()

	for i, entry := range entries {

		entry.Info.TIDs = make([]byte, pdi.EntryTID_NormalNumTIDs * plan.TIDSz)
		entry.Info.EntryID().SetTimeFS(nowFS)

		if ! entry.whitelist {
			copy(entry.Info.ACCEntryID(),			newACC.Info.EntryID())

			if entry.parentEntry != nil {
				copy(entry.Info.ChannelEpochID(),	entry.parentEntry.Info.EntryID())
			}
		}

		var body []byte

		if entry.chEpoch != nil {
			entry.Info.EntryOp = pdi.EntryOp_NEW_CHANNEL_EPOCH

			body, _ = entry.chEpoch.Marshal()
		} else {
			entry.Info.EntryOp = pdi.EntryOp_POST_CONTENT

			body, _ = entry.body.Marshal()
		}

		txns, err := mc.EncryptAndEncodeEntry(&entry.Info, body)
		if err != nil {
			return err
		}

		entryID := entry.Info.EntryID()

		// Set the channel IDs of the newly generated community channels
		if i < 3 {
			CG.Infof(0, "Created %s: %s", pdi.CommunityChID_name[int32(entry.assignTo)], entryID.Str())
			copy(CG.GenesisSeed.StorageEpoch.CommunityChID(entry.assignTo), entryID.ExtractChID())
		}

		if entry.whitelist {
			CG.GenesisSeed.StorageEpoch.GenesisEntryIDs = append(CG.GenesisSeed.StorageEpoch.GenesisEntryIDs, entryID)
		}

		if entry == newCommunityHome {
			CG.GenesisSeed.CommunityEpoch.Links = append(CG.GenesisSeed.CommunityEpoch.Links, &plan.Link{
				Label: "home",
				Uri: fmt.Sprintf("/plan/./%s/ChID/%s", entry.chEpoch.ChProtocol, plan.BinEncode(entryID.ExtractChID())),
			})
		}

		for _, seg := range txns.Segs {
			CG.txnsToCommit = append(CG.txnsToCommit, pdi.RawTxn{
				Bytes: seg.RawTxn,
			})
		}

	}

	// Set the member epoch ID now that we know it.
	CG.MemberSeed.MemberEpoch.EpochTID = postMember.Info.EntryID()

	// Set the genesis community epoch ID now that the entry ID has been generated
	CG.GenesisSeed.CommunityEpoch.EpochTID = postGenesisEpoch.Info.EntryID()

	return nil
}



func promptForPass() (string, error) {
	return "pass1234", nil
/*
	reader := bufio.NewReader(os.Stdin)
			fmt.Println("ENTER TO START")
			reader.ReadString('\n')*/
}
