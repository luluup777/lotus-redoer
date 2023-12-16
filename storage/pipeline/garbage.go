package sealing

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/storage/pipeline/lib/nullreader"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

func (m *Sealing) PledgeSector(ctx context.Context) (storiface.SectorRef, error) {
	m.startupWait.Wait()

	m.inputLk.Lock()
	defer m.inputLk.Unlock()

	cfg, err := m.getConfig()
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() >= cfg.MaxSealingSectors {
			return storiface.SectorRef{}, xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	spt, err := m.currentSealProof(ctx)
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("getting seal proof type: %w", err)
	}

	sid, err := m.createSector(ctx, cfg, spt)
	if err != nil {
		return storiface.SectorRef{}, err
	}

	log.Infof("Creating CC sector %d", sid)
	return m.minerSector(spt, sid), m.sectors.Send(uint64(sid), SectorStartCC{
		ID:         sid,
		SectorType: spt,
	})
}

func (m *Sealing) RedoSector(ctx context.Context, sid int) error {
	si, err := m.GetSectorInfo(abi.SectorNumber(sid))
	if err != nil {
		return err
	}
	if si.State != Proving {
		return fmt.Errorf("the sector state: %s is not Proving", si.State)
	}

	if ok := m.recordSector(sid); !ok {
		return errors.New("sector recovering")
	}

	err = m.pledgeSector(ctx, abi.SectorNumber(sid))
	if err != nil {
		return err
	}

	return m.recovery(abi.SectorNumber(sid))
}

func (m *Sealing) recordSector(sid int) bool {
	m.redoLk.Lock()
	defer m.redoLk.Unlock()

	if _, ok := m.redoingSectors[sid]; ok {
		return false
	} else {
		m.redoingSectors[sid] = struct{}{}
		return true
	}
}

func (m *Sealing) pledgeSector(ctx context.Context, sid abi.SectorNumber) error {
	spt, err := m.currentSealProof(ctx)
	if err != nil {
		return xerrors.Errorf("getting seal proof type: %w", err)
	}

	sectorID := m.minerSector(spt, sid)

	size, err := spt.SectorSize()
	if err != nil {
		return err
	}

	paddedSize := abi.PaddedPieceSize(size).Unpadded()

	log.Infof("AddPiece %d", sectorID)

	_, err = m.sealer.AddPiece(ctx, sectorID, []abi.UnpaddedPieceSize{}, paddedSize, nullreader.NewNullReader(paddedSize))
	if err != nil {
		return xerrors.Errorf("add piece: %w", err)
	}

	return nil
}

func (m *Sealing) recovery(sid abi.SectorNumber) error {
	log.Infow("recovery", "sid", sid)
	return m.sectors.Send(uint64(sid), SectorForceState{
		State: Packing,
	})
}
