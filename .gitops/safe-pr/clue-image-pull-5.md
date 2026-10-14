# fix(gitops): clue-image-pull-5 이미지 복구 제안

ImagePullBackOff 복구 제안. 사건 ID: clue-image-pull-5. 이미지 하나만 변경한다. 검토 후 반영하며 실제 클러스터의 새 증거로 복구 여부를 확인한다.

- manifest_path: `deploy/oss/live-recovery.yaml`
- pr_kind: `safe_pr_manifest_edit`
- workflow_run_id: `clue-image-pull-5`
- environment: `sandbox`

## Evidence

- commit_sha: `bd7cce96f52179086ab373a59ae6e977d2396b6d`
- patch_sha256: `sha256:aaef366e428a52d64a9534950bbe581e7899b5baf8c1ffccaec9ba59e1d9e324`

## Approval

- approval_ref: `clue-image-pull-5-proposal`

## Files

- `deploy/oss/live-recovery.yaml`: manifest patch
