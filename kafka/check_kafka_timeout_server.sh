#!/bin/bash

# 13개 서버 목록 정의 (서버 주소 또는 호스트명으로 수정 필요)
SERVERS=(
    "server1"
    "server2"
    "server3"
    "server4"
    "server5"
    "server6"
    "server7"
    "server8"
    "server9"
    "server10"
    "server11"
    "server12"
    "server13"
)

# Kafka 서버 포트 (기본값: 9092)
KAFKA_PORT=9092

# 연결 타임아웃 시간 (초)
TIMEOUT=5

# 결과를 저장할 배열
TIMEOUT_SERVERS=()

echo "=========================================="
echo "Kafka Timeout 서버 검색 시작"
echo "=========================================="
echo ""

# 각 서버에 대해 연결 테스트 수행
for SERVER in "${SERVERS[@]}"; do
    echo "[연결 테스트 중] $SERVER:$KAFKA_PORT..."
    
    # nc -z를 사용하여 실제 Kafka 서버 연결 테스트
    # -z: 연결만 확인하고 데이터 전송 안함 (포트 스캔 모드)
    # -w: 연결 대기 타임아웃 설정 (초)
    # timeout: 전체 명령 실행 시간 제한 (초)
    if timeout $TIMEOUT nc -z -w $TIMEOUT "$SERVER" "$KAFKA_PORT" 2>/dev/null; then
        echo "  ✓ $SERVER: Kafka 연결 성공"
    else
        echo "  ✗ $SERVER: Kafka 연결 실패 또는 timeout 발생!"
        TIMEOUT_SERVERS+=("$SERVER")
    fi
done

echo ""
echo "=========================================="
echo "검색 완료"
echo "=========================================="
echo ""

# 결과 출력
if [ ${#TIMEOUT_SERVERS[@]} -eq 0 ]; then
    echo "모든 서버에서 Kafka 연결이 정상입니다."
else
    echo "Kafka 연결 실패 또는 timeout이 발생한 서버 목록:"
    for SERVER in "${TIMEOUT_SERVERS[@]}"; do
        echo "  - $SERVER:$KAFKA_PORT"
    done
    echo ""
    echo "총 ${#TIMEOUT_SERVERS[@]}개 서버에서 Kafka 연결 문제 발견"
fi

