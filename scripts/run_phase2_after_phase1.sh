#!/bin/bash
# =============================================================================
# run_phase2_after_phase1.sh
# Phase 1 완료 후 자동으로 report → Phase 2를 실행합니다.
# 사용법: bash run_phase2_after_phase1.sh
# =============================================================================

PHASE1_PID=23871
LOG_FILE="results/phase2_autorun.log"
VENV_PYTHON=".venv/bin/python"

cd "$(dirname "$0")"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Phase 2 자동 실행 대기 시작 (Phase 1 PID: $PHASE1_PID)" | tee -a "$LOG_FILE"

# -------------------------------------------------------------------
# 1) Phase 1 프로세스가 끝날 때까지 polling으로 대기
#    (wait는 자식 프로세스에만 동작하므로 kill -0으로 폴링)
# -------------------------------------------------------------------
while kill -0 "$PHASE1_PID" 2>/dev/null; do
    RESULT_COUNT=$(tail -n +2 results/results.csv 2>/dev/null | grep "^1," | wc -l | tr -d ' ')
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Phase 1 실행 중... ($RESULT_COUNT/450 완료)" | tee -a "$LOG_FILE"
    sleep 60
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Phase 1 프로세스 종료 감지" | tee -a "$LOG_FILE"

# -------------------------------------------------------------------
# 2) Phase 1 결과 확인
# -------------------------------------------------------------------
RESULT_COUNT=$(tail -n +2 results/results.csv 2>/dev/null | grep "^1," | wc -l | tr -d ' ')
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Phase 1 완료된 실험 수: $RESULT_COUNT / 450" | tee -a "$LOG_FILE"

# -------------------------------------------------------------------
# 3) Phase 1 리포트 생성
# -------------------------------------------------------------------
echo "" | tee -a "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Phase 1 리포트 생성 ===" | tee -a "$LOG_FILE"
$VENV_PYTHON report.py 2>&1 | tee -a "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 리포트 생성 완료" | tee -a "$LOG_FILE"

# -------------------------------------------------------------------
# 4) Phase 2 실행
# -------------------------------------------------------------------
echo "" | tee -a "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Phase 2 시작 (실제 PQC 서명) ===" | tee -a "$LOG_FILE"
$VENV_PYTHON main.py --phase 2 2>&1 | tee -a "$LOG_FILE"
PHASE2_EXIT=$?

echo "" | tee -a "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Phase 2 종료 (exit code: $PHASE2_EXIT)" | tee -a "$LOG_FILE"

# -------------------------------------------------------------------
# 5) 최종 리포트 생성
# -------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] === 최종 리포트 생성 ===" | tee -a "$LOG_FILE"
$VENV_PYTHON report.py 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" | tee -a "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 모든 실험 완료! 결과: results/results.csv" | tee -a "$LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" | tee -a "$LOG_FILE"
