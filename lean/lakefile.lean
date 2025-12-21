import Lake
open Lake DSL

package leanGuard where

lean_lib LeanGuard where

@[default_target]
lean_exe dcqcn_check where
  root := `LeanGuard.Main

lean_exe pfc_check where
  root := `LeanGuard.PfcMain
