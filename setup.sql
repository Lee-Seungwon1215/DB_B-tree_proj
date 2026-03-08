-- PostgreSQL 초기 설정 SQL
-- 실험 시작 전 한 번만 실행하면 됩니다.

-- B+tree 깊이 측정에 필요한 확장 설치
CREATE EXTENSION IF NOT EXISTS pageinspect;

-- 캐시 히트율 / I/O 측정에 필요한 확장 설치
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
