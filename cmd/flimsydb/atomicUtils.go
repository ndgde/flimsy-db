package flimsydb

import (
	"fmt"

	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

/*
all functions have a small flaw, there is only one rollback
error, when in fact there may be several of them, and all
of them should be displayed as one error with an enumeration
*/

/*
the function requires that the passed column row values
not be changed during its execution, the function requires
that during its execution the passed column rows do not
change their value
*/
func IdxrAddRow(scheme Scheme, row Row, index int) error {
	completedOps := make(map[*Column]cm.Blob)
	for i, col := range scheme {
		if col.IdxrType == indexer.AbsentIndexerType {
			continue
		}

		if err := col.Idxr.Add(row[i], index); err != nil {
			var rollbackErr error
			for col, v := range completedOps {
				if rErr := col.Idxr.Delete(v, index); rErr != nil {
					rollbackErr = fmt.Errorf("rollback failed for column %v: %w", col, rErr)
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("error adding index and rollback failed: %w", rollbackErr)
			}
			return fmt.Errorf("error adding index: %w", err)
		}

		completedOps[col] = row[i]
	}

	return nil
}

func IdxrUpdateRow(scheme Scheme, oldRow Row, newRow Row, index int) error {
	updatedOps := make(map[*Column]cm.Blob)
	for i, col := range scheme {
		if col.IdxrType == indexer.AbsentIndexerType {
			continue
		}

		if err := col.Idxr.Update(oldRow[i], newRow[i], index); err != nil {
			var rollbackErr error
			for col, v := range updatedOps {
				if rErr := col.Idxr.Update(newRow[i], v, index); rErr != nil {
					rollbackErr = fmt.Errorf("rollback failed for column %v: %w", col, rErr)
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("error updating index and rollback failed: %w", rollbackErr)
			}
			return fmt.Errorf("error updating index: %w", err)
		}

		updatedOps[col] = oldRow[i]
	}

	return nil
}

func IdxrDeleteRow(scheme Scheme, row Row, index int) error {
	deletedOps := make(map[*Column]cm.Blob)
	for i, col := range scheme {
		if col.IdxrType == indexer.AbsentIndexerType {
			continue
		}

		if err := col.Idxr.Delete(row[i], index); err != nil {
			var rollbackErr error
			for col, v := range deletedOps {
				if rErr := col.Idxr.Add(v, index); rErr != nil {
					rollbackErr = fmt.Errorf("rollback failed for column %v: %w", col, rErr)
				}
			}
			if rollbackErr != nil {
				return fmt.Errorf("error deleting index and rollback failed: %w", rollbackErr)
			}
			return fmt.Errorf("error deleting index: %w", err)
		}

		deletedOps[col] = row[i]
	}

	return nil
}
