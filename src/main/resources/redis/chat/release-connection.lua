-- 인증 성공·업그레이드 실패·취소 모두 같은 리스를 멱등 해제한다. 장애 시에는 TTL로 정리된다.
redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('ZCARD', KEYS[1]) == 0 then redis.call('DEL', KEYS[1]) end
return 1
