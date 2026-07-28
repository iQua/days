import Lake
open Lake DSL

package leanGuard where

lean_lib LeanGuard where

@[default_target]
lean_lib DaysExecutor where

@[default_target]
lean_exe dcqcn_check where
  root := `LeanGuard.Main

lean_exe pfc_check where
  root := `LeanGuard.PfcMain

lean_exe cubic_check where
  root := `LeanGuard.CubicMain

lean_exe drr_check where
  root := `LeanGuard.DrrMain

lean_exe wfq_check where
  root := `LeanGuard.WfqMain

lean_exe aqm_check where
  root := `LeanGuard.AqmMain

lean_exe aqm_dcqcn_check where
  root := `LeanGuard.AqmDcqcnMain
