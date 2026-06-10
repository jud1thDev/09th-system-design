# Week 6 과제: 공모주 청약 시스템 설계

- 청약 마감일, 수백만 명이 동시에 증거금을 납입하는 상황에서 데이터 정합성을 어떻게 보장할지 설계합니다.
- 증거금 차감, 청약 수량 배정, 초과 신청분 환불처럼 여러 단계에 걸친 금융 처리 흐름에서 트랜잭션, 락, 멱등성, Outbox Pattern, 이벤트 기반 후처리, 장애 복구 전략을 비교합니다.
- 한정된 공급(주식 수량)에 폭발적 수요(동시 청약)가 몰리는 상황에서, 실시간성보다 정확성이 중요한 금융 도메인의 아키텍처 선택이 어떻게 달라지는지 정리합니다.
- (선택 실습) 트랜잭션 보장 흐름 일부를 구현합니다.

---

## ⒈ 문제 이해 및 설계 범위 확정

### 시나리오

당신은 국내 증권사의 백엔드 엔지니어로, 공모주 청약 시스템을 담당하고 있다.

**공모주 청약이란?**
기업이 주식 시장에 처음 상장할 때(IPO), 일반 투자자가 공개된 가격으로 주식을 미리 신청하는 절차다. 투자자는 원하는 수량만큼 주식을 신청하고, 신청 수량에 비례하는 증거금(보증금)을 미리 납입한다. 청약 마감 후 실제 배정 수량이 결정되며, 배정받지 못한 수량의 증거금은 환불된다.

```
예시: A기업 공모주 청약
- 청약 기간: 3일 (마지막 날 오후 4시 마감)
- 공모 주식 수량: 1,000만 주
- 청약 단가: 주당 50,000원
- 청약 경쟁률: 최종 500:1 (실제 신청량 = 공급의 500배)
```

사용자가 청약 버튼을 누르면 다음이 순차적으로 일어나야 한다.
```
1. 청약 요청 접수 (중복 청약 방지, 청약 자격 검증)
2. 증거금 차감 (계좌 잔액에서 납입 금액 차감)
3. 청약 내역 기록
4. 청약 마감 후 → 배정 수량 계산
5. 배정된 수량만큼 주식 지급
6. 미배정 증거금 환불
7. 청약 결과 알림 발송
```

예를 들어 이런 상황이 발생할 수 있다.

```
- 청약 마감 직전 수십만 명이 동시에 버튼을 누르면?
- 증거금은 차감됐는데 청약 내역 기록 전에 서버가 죽으면?
- 네트워크 오류로 청약 요청이 두 번 들어오면?
- 환불 처리 중 외부 은행 API가 다운되면?
- 배정 계산 도중 일부 사용자 데이터가 누락되면?
```

본 시스템은 이러한 상황에서도 증거금이 정확히 한 번만 차감되고,
배정과 환불이 한 명도 빠짐없이 정확하게 처리되도록 설계한다.

그 외 시나리오는 자유롭게 구체화해도 좋다.

```
- 채권 청약 시스템
- 리츠(부동산 공모) 청약 시스템
- 크라우드펀딩 플랫폼 선착순 투자
- 토큰증권(STO) 공모 시스템
```

---

## 설계 범위 (In / Out of Scope)

| 포함 (In Scope) | 제외 (Out of Scope) |
| --- | --- |
| 청약 요청 처리 흐름 전체 | 주식 시장 상장 심사 프로세스 |
| 증거금 차감 / 환불 정합성 | 주가 산정 및 공모가 결정 |
| 중복 청약 방지 (멱등성) | AML / 이상거래 탐지 모델 |
| 동시 청약 요청 락 전략 | KYC / 투자자 적합성 심사 |
| 배정 수량 계산 및 결과 저장 | 증권사 HTS / MTS UI 구현 |
| Outbox Pattern 기반 이벤트 발행 | 완전한 보안 솔루션 |
| 이벤트 기반 후처리 (알림, 정산) | 실제 코어뱅킹 연동 구현 |
| 청약 마감 후 대량 환불 처리 | 세금 / 수수료 계산 시스템 |
| 장애 복구 및 보상 트랜잭션 | 주식 계좌 개설 프로세스 |
| 피크 트래픽 대응 전략 | 타 증권사 연동 시스템 |

---

## 시스템 구성 전제

- 투자자는 이미 계좌 개설 및 본인인증이 완료된 상태라고 가정한다.
- 투자자의 증거금 계좌 잔액은 내부 DB에 저장되어 있다고 가정한다.
- 외부 은행 API(환불 송금용)는 별도 시스템으로 존재하며, 응답 지연 및 실패가 발생할 수 있다.
- 알림 서비스(푸시, SMS, 이메일)는 별도 마이크로서비스로 분리되어 있다.
- 메시지 브로커(Kafka 등)는 사용 가능하다고 가정한다.
- 배정 계산은 청약 마감 후 배치로 수행된다고 가정한다.
- 본 시스템은 청약 정합성, 멱등성, 피크 트래픽 처리, 장애 복구를 책임진다.

---

## 기능 요구사항

- 투자자의 청약 요청을 접수하고 증거금을 차감할 수 있어야 한다.
- 동일한 청약 요청이 중복으로 들어와도 한 번만 처리되어야 한다 (멱등성).
- 한 투자자가 동일 공모주에 중복 청약하는 것을 방지해야 한다.
- 증거금 차감과 청약 내역 기록은 원자적으로 처리되어야 한다.
- 청약 마감 후 배정 수량을 계산하고 결과를 저장할 수 있어야 한다.
- 미배정 증거금은 전액 환불되어야 하며 누락이 없어야 한다.
- 외부 시스템(알림, 은행 환불 API) 장애가 청약 핵심 처리를 실패시켜서는 안 된다.
- 청약 상태(접수 중 / 마감 / 배정 완료 / 환불 완료)를 투자자가 확인할 수 있어야 한다.
- 청약 마감 직전 트래픽 폭발 상황에서도 시스템이 안정적으로 동작해야 한다.

---

## 비기능 요구사항

| 항목 | 목표 |
| --- | --- |
| 청약 접수 응답 시간 | p95 1초 이내 |
| 증거금 정합성 | 이중 차감 / 환불 누락 발생 불가 |
| 멱등성 보장 | 동일 요청 N회 재시도 시 결과 동일 |
| 피크 트래픽 처리 | 마감 직전 평시 대비 50배 트래픽 처리 |
| 외부 API 타임아웃 대응 | 초과 시 비동기 처리 전환 |
| 환불 처리 완료 시간 | 배정 확정 후 24시간 이내 전원 완료 |
| 이벤트 유실 허용 범위 | 청약 / 환불 이벤트 유실 불가 |
| 장애 복구 | 서버 재시작 후 미완료 처리 자동 재개 |
| 시스템 가용성 | 청약 기간 중 월 99.99% 이상 |
| 청약 내역 보관 | 5년 이상 (금융 규제 기준) |

---

## 대략적 규모 추정 *(기준값 — 본인 가정으로 변경 가능)*

| 항목 | 수치 |
| --- | --- |
| 공모주 청약 참여 투자자 수 | 약 3,000,000명 (인기 종목 기준) |
| 청약 기간 | 3일 (마지막 날 트래픽 집중) |
| 마감 직전 1시간 청약 요청 비율 | 전체의 약 40% (약 1,200,000건) |
| 피크 QPS | 약 3,000 ~ 10,000 TPS |
| 평시 QPS | 약 100 ~ 300 TPS |
| 청약 1건당 처리 단계 수 | 약 4단계 (검증 → 차감 → 기록 → 이벤트) |
| 마감 후 환불 대상 건수 | 약 2,500,000건 (경쟁률 500:1 기준) |
| 환불 처리 목표 시간 | 24시간 이내 전원 완료 |
| 외부 은행 API 처리 한계 | 초당 약 500건 |
| 거래 내역 데이터 보관 기간 | 5년 이상 |
| 피크 시간대 | 청약 마감일 오후 3시 ~ 4시 |

---

# 2. 개략적 설계안 제시 및 동의 구하기

---

## 핵심 흐름 (필수)

현재 설계의 핵심 판단은 다음과 같다.

- 청약 접수 성공 기준은 `ConfirmSubscription` 코어 트랜잭션(T2)의 DB commit이다.
- `Subscription Conductor`는 별도 요청 접수 트랜잭션(T1)을 만들지 않고, T2의 얇은 진입점으로 둔다.
- API와 Conductor 사이에는 핵심 경로용 MQ를 두지 않는다. MQ를 두면 API 응답 의미가 "청약 완료"가 아니라 "요청 접수"로 바뀌기 때문이다.
- 마감 직전 트래픽은 API 뒤 큐가 아니라 API 앞 Waiting Room / Admission Gate에서 흡수한다.
- 증거금은 계좌 총액에서 바로 사라지는 것이 아니라 `available_balance -> held_balance`로 이동한다.
- 배정은 실제 받을 주식 수량과 정산 금액을 확정하는 단계이고, 정산은 `DEPOSIT_FINALIZED` 원장 이벤트로 정확히 한 번 수행한다.
- 이벤트 발행은 CDC Outbox를 기본 후보로 둔다. 애플리케이션은 DB 트랜잭션 안에서 outbox row를 append하고, CDC connector가 Kafka producer 역할을 한다.

청약 등록의 동기 경로는 다음과 같다.

``` 
Client
-> Waiting Room / Admission Gate
-> Subscription API
-> Subscription Conductor
-> ConfirmSubscription UseCase
-> Oracle T2 Commit
-> API Response
```

T2 안에서는 다음 작업이 함께 성공하거나 함께 rollback된다.

``` 
idempotency 확인/생성
offer 상태 확인
account 조건부 update 또는 row write lock
available_balance 감소
held_balance 증가
deposit_hold 생성
ledger DEPOSIT_HELD 생성
subscription CONFIRMED 저장
outbox SubscriptionConfirmed append
```

## 개략적 아키텍처 다이어그램 (필수)

현재 후보로 남겨둔 인프라 전략은 두 가지다.

- 고정 용량 pool
  - 설명: LB / NAT Pool / API / Conductor를 평상시부터 안정 용량으로 유지한다.
  - 장점: 예측 가능하고, scale-in 리스크가 없으며, DB connection budget을 고정할 수 있다.
  - 단점: 유휴 비용이 크고 capacity planning이 중요하다.
- SW LB + autoscaling
  - 설명: SW LB/API/Conductor를 확장 가능하게 두되 admission control로 보호한다.
  - 장점: 자원 효율과 확장 유연성이 좋다.
  - 단점: LB 반응 속도, graceful drain, cold start, connection 폭증을 고려해야 한다.

두 후보 모두 API 앞 Waiting Room과 admission token은 필요하다. Autoscaling은 피크 제어의 주 수단이 아니라 보조 수단으로 본다.

---

# 3. 상세 설계

---

## 상세 아키텍처 다이어그램

<img width="6539" height="8191" alt="Untitled diagram-2026-06-10-121059" src="https://github.com/user-attachments/assets/4c6425b1-1071-43ef-89a8-2db62e1082d1" />

- T2 이후 DB를 안 쓰는 게 아님
- T2 이후에도 배정, 정산, 환불은 모두 DB 트랜잭션으로 처리함
- Outbox는 각 트랜잭션의 성공 사실을 다음 단계로 유실 없이 넘기는 장치임
- 
  
---

## 설계 대상 컴포넌트 사이의 우선순위 정하기 / 아키텍처 다이어그램 (필수)

- 1순위: Oracle Primary RDB
  - 청약, 계좌, 원장, hold, outbox의 source of truth다.
- 2순위: ConfirmSubscription UseCase
  - T2의 실제 트랜잭션 owner다.
- 3순위: Subscription Conductor
  - T2 진입점이다. 별도 T1 commit은 만들지 않는다.
- 4순위: Account / Hold / Ledger Domain
  - `available -> held`, `DEPOSIT_HELD`, `DEPOSIT_FINALIZED` 정합성을 담당한다.
- 5순위: Oracle AQ or CDC Outbox + Kafka
  - commit된 도메인 이벤트를 후속 컴포넌트로 전달한다.
- 6순위: Allocation / Settlement Workers
  - 배정 결과 확정과 대량 정산을 병렬 처리한다.
- 7순위: Read Model / Notification
  - 조회와 알림을 담당한다. 핵심 거래보다 후순위다.

---

## 3-1. 피크 트래픽을 어떻게 처리할 것인가?

- API와 Conductor 사이에 MQ를 둘 것인가?
   -> 두지 않는다. MQ를 두면 API 응답이 "청약 완료"가 아니라 "요청 접수"로 바뀌고, queue drain과 중간 상태 관리가 필요해진다.
- MQ 없이 피크를 어떻게 흡수할 것인가?
   -> API 앞 Waiting Room / Admission Gate에서 흡수한다.
- token bucket을 초과한 정상 사용자는 어떻게 처리할 것인가?
   -> 버리지 않고 Waiting Room에 둔다. token bucket은 거절 기준이 아니라 API 방출 속도 제어다.
- LB/API/Conductor는 고정 pool인가 autoscaling인가?
   -> 두 후보를 모두 둔다. 핵심은 어떤 후보든 admission control로 Oracle T2 진입량을 제한하는 것이다.

정리하면 피크 트래픽은 다음 구조로 방어한다.

``` 
Client -> LB/NAT Pool -> Waiting Room -> API Gateway -> Subscription API -> Conductor -> Oracle T2
```

- Waiting Room 입장은 청약 성공이 아니다.
- Admission Token은 API 호출 권한일 뿐이다.
- 청약 성공은 T2 commit 이후에만 선언한다.
- 고정 용량 pool은 예측 가능성이 높지만 유휴 비용이 크다.
- SW LB + autoscaling은 유연하지만 LB 반응 속도, graceful drain, DB connection 폭증을 관리해야 한다.

---

## 3-2. 증거금 동시성 제어는 어떻게 할 것인가?
- Pessimistic Lock과 Optimistic Lock 중 무엇을 선택할 것인가?
   -> 비관적 동시성 제어를 선택한다.
- 꼭 `SELECT FOR UPDATE`를 써야 하는가?
   -> 계좌 잔액처럼 정합성이 중요한 구간은 `SELECT ... FOR UPDATE`로 명시적 row lock을 잡는다.
  - 조건부 update는 잔액 부족을 한 번에 판정할 수 있는 보조 방식이지만, 본 설계의 기준 표현은 `SELECT ... FOR UPDATE`로 둔다.
- 낙관락은 왜 선택하지 않는가?
   -> 충돌 후 재시도/응답/멱등성 처리를 애플리케이션이 더 많이 떠안고, 마감 직전 재시도 폭주를 만들 수 있다.

증거금 계좌는 다음처럼 나눈다.
- `available_balance`: 사용 가능한 증거금
- `held_balance`: 청약 때문에 묶인 증거금

청약 시점에는 외부 송금이 아니라 `available -> held` 이동을 수행한다.

- `available_balance -= deposit_amount`
- `held_balance += deposit_amount`

같은 계좌의 동시 청약은 Oracle의 account row write lock으로 직렬화한다. affected row가 0이면 잔액 부족으로 처리한다. Redis 같은 분산 락은 정합성 기준이 아니라 반복 요청 완화용 보조 수단으로만 본다.

---

## 3-3. 중복 청약을 어떻게 방지할 것인가?

- 같은 key 동시 요청, 같은 key 다른 payload, 다른 key 중복 청약은 어떻게 처리하는가?
   -> 최초로 T2 commit에 성공한 청약만 유효하다.
- 같은 key 다른 payload는 수정인가?
   -> 수정으로 보지 않는다. 같은 idempotency key의 다른 payload는 `IdempotencyKeyConflict`다.
- 청약 수정/취소도 설계하는가?
   -> 이번 범위에서 제외한다.

중복은 두 종류로 나눈다.
- 기술적 중복
  - 같은 사용자 행동의 재시도다.
  - 같은 `idempotency_key`면 기존 결과를 반환한다.
- 비즈니스 중복
  - 같은 투자자가 같은 공모주에 새 청약을 시도하는 경우다.
  - `UNIQUE(offer_id, investor_id)`로 하나만 성공시킨다.

필수 제약:
``` 
UNIQUE(investor_id, idempotency_key) # 하나의 청약 요청이 중복 처리 됨을 방지
UNIQUE(offer_id, investor_id) # 한 투자자가 중복 요청을 하는 것을 방지
UNIQUE(subscription_id, DEPOSIT_HELD) # 중복 차감을 방지
```

`idempotency_requests`에는 `investor_id`, `idempotency_key`, `request_hash`, `status`, `subscription_id`, `response_code`, timestamp를 저장한다.
- 같은 key + 같은 payload: 기존 결과 반환
- 같은 key + 다른 payload: `409 IdempotencyKeyConflict`
- 다른 key + 같은 offer/investor: 기존 청약 반환 또는 중복 청약 거절
- 잔액 부족/마감/자격 없음: `FAILED_FINAL`로 저장
- DB timeout/일시 장애: 실패 확정으로 저장하지 않고 같은 key 재시도 허용

---

## 3-4. Outbox Pattern으로 이벤트를 어떻게 안전하게 발행할 것인가?

- Kafka는 청약 성공 응답을 줄 수 있는가?
   -> 아니다. Kafka ack는 메시지 저장 성공이고, 청약 성공은 T2 commit이 결정한다.
- Kafka를 쓰는 이유는 무엇인가?
   -> 단순 fanout뿐 아니라 durable event stream, replay, consumer 독립성을 얻기 위해 사용한다.
- CDC Outbox는 무엇인가?
   -> 애플리케이션 producer 대신 CDC connector가 DB commit log를 읽어 Kafka로 전달하는 방식이다.
- Oracle이면 어떤 방식이 가장 자연스러운가?
  -> Oracle AQ + Kafka Bridge를 1순위 후보로 둔다. CDC Outbox는 벤더 중립성과 PostgreSQL 호환성을 강조할 때 좋은 후보가 된다.

이벤트 전달 후보
- Oracle
  - 1순위: Oracle AQ + Kafka Bridge
  - 2순위: CDC Outbox
  - fallback: Polling Outbox
- PostgreSQL
  - 1순위: CDC Outbox
  - 2순위: Polling Outbox
  - 직접 publish + reconciler는 비추천

최소 이벤트
- `SubscriptionConfirmed`: T2 commit 완료
- `OfferClosed`: 신규 청약 차단, 배정 snapshot 준비
- `AllocationCompleted`: 배정 결과 저장 완료
- `DepositFinalized`: held 증거금 정산 완료
- `SubscriptionSettled`: 청약 단위 정산/주식 지급 완료
- `ReviewRequired`: 자동 처리가 위험해 수동 확인 필요

CDC/Kafka는 핵심 비즈니스 판단 기준이 아니라 이벤트 전달자다. 그러나 이 이벤트들이 다음 액션을 트리거 하므로, CDC lag와 connector 장애는 핵심 운영 지표로 본다.

---

## 3-5. 마감 후 대량 환불을 어떻게 처리할 것인가?

- 환불은 외부 은행 송금인가, 내부 증거금 release인가?
   -> 이번 과제의 핵심 환불은 내부 증거금 계좌 release다. 외부 은행 송금은 내부 정산 이후 별도 단계로 분리한다.
- `held_balance`는 무엇인가?
   -> 이미 `available_balance`에서 빠져 청약 정산 전까지 묶여 있는 돈이다.
- 중복 정산은 어떻게 막는가?
   -> `UNIQUE(subscription_id, DEPOSIT_FINALIZED)`로 같은 청약의 최종 정산 이벤트가 두 번 존재할 수 없게 한다.

청약 시:

``` 
available_balance -= deposit_amount
held_balance += deposit_amount
deposit_hold.status = HELD
ledger = DEPOSIT_HELD
```

정산 시:

- `deposit_hold`를 잠그고 배정 결과를 조회한다.
- 배정 금액과 환불 금액을 계산한다.
- `DEPOSIT_FINALIZED` 원장 이벤트를 생성한다.
- 이 이벤트가 새로 생성된 경우에만 `held_balance`를 줄이고 `available_balance`를 환불 금액만큼 늘린다.
- 같은 트랜잭션에서 `deposit_hold = FINALIZED`, `subscription = SETTLED`로 변경한다.
- 배정 수량은 position ledger에 반영한다.

이미 `DEPOSIT_FINALIZED`가 있으면 account balance update를 다시 수행하지 않는다. 외부 은행 송금이 필요하면 내부 정산 이후 별도 Saga로 처리하며, timeout은 실패가 아니라 불명확 상태로 본다.

---

## 3-6. 장애 복구 및 미완료 처리

- 상태는 단순하게 둘 것인가, 복잡하게 둘 것인가?
   -> 외부 노출 상태는 단순하게, 내부 상태는 복구와 감사에 필요한 만큼만 세분화한다.
- CDC/Kafka 중복은 어떻게 처리하는가?
   -> at-least-once를 전제로 consumer를 멱등하게 만든다.

사용자 노출 상태:

- `CONFIRMED`: 청약 접수 완료
- offer `CLOSED` + subscription `CONFIRMED`: 배정 대기
- `ALLOCATED`: 배정 완료
- `SETTLED`: 정산/환불 완료
- `REJECTED`: 청약 실패
- `REVIEW_REQUIRED`: 확인 필요

복구 기준:

- T2 commit 전 장애: DB rollback, 같은 idempotency key 재시도
- T2 commit 후 응답 유실: idempotency `SUCCEEDED`로 기존 결과 반환
- T2 commit 후 이벤트 지연: AQ/CDC lag 모니터링, consumer는 지연 허용
- Allocation shard 실패: shard job timeout 후 재시도
- Settlement item 실패: `DEPOSIT_FINALIZED` unique 기준으로 재시도
- Consumer 중복 처리: event_id 또는 업무 unique key로 멱등 upsert

---

## 3-7. 배정 계산의 정합성을 어떻게 보장할 것인가?

- 배정은 무엇인가?
   -> 신청 수량 중 실제로 몇 주를 받을지 확정하는 단계다. 증거금 hold는 이미 청약 시점에 완료되어 있다.
- 배정 공식까지 설계하는가?
   -> 균등/비례 공식은 깊게 다루지 않는다. snapshot, 누락 방지, 재실행 가능성, 정산 연결을 설계한다.
- 병렬 처리해도 되는가?
   -> 청약은 샤딩 단위 병렬 처리는 가능하다. 단, 전체 공급량 제약은 allocation job 단위로 검증한다.

배정 대상:

``` 
offer.status = CLOSED
subscription.status = CONFIRMED
confirmed_at <= offer.closed_at
```

배정 결과 저장은 `hash(subscription_id) % N` 기준 병렬 처리한다. 단, 최종 완료 전에는 다음을 검증한다.

- `sum(allocated_quantity) <= offer.total_shares`
- `sum(allocated_amount + refund_amount) = sum(subscription.deposit_amount)`
- `refund_amount >= 0`
- `allocated_amount <= deposit_amount`

`AllocationCompleted` 이벤트에는 250만 건의 결과를 넣지 않는다. `offer_id`, `allocation_job_id`, `snapshot_version`, 합계만 넣고 정산 worker가 DB에서 shard 단위로 읽는다.

---

## 3-8. 청약 내역 저장 및 대용량 조회

- 쓰기와 읽기를 어떻게 분리하는가?
   -> 쓰기는 Oracle Primary, 조회는 Read Model / Redis로 분리한다.
- 대량 집계는 어떻게 줄이는가?
   -> 배정/정산은 태스크/샤드 단위로 처리하고, 조회성 집계는 Kafka projection / Redis aggregate로 분리한다.
- 장기 보관은 어떻게 하는가?
   -> 공모주 단위 또는 월 단위 파티셔닝을 두고, 원장과 청약 내역은 5년 이상 보관한다.
- Projection consumer는 중복 이벤트를 받을 수 있으므로 upsert로 멱등 처리한다.

---

# 4. 설계 장점

- 청약 성공 기준을 T2 commit으로 정의해 사용자 응답과 DB 상태의 의미가 명확하다.
- Conductor를 T2 진입점으로 제한해 역할 비대화를 막는다.
- 증거금을 `available -> held`로 모델링해 환불을 내부 release로 설명할 수 있다.
- `DEPOSIT_HELD`, `DEPOSIT_FINALIZED` unique 원장으로 중복 반영 가능성을 DB 제약상 차단한다.
- API 뒤 MQ 대신 API 앞 Waiting Room을 사용해 동기 청약 확정 구조를 유지한다.
- Oracle AQ/CDC Outbox로 DB commit된 도메인 이벤트만 Kafka로 전달한다.
- 배정과 정산을 태스크/샤드 단위로 병렬화할 수 있다.
- 조회 모델을 분리해 마감 직전 조회 폭주가 Oracle write path를 압박하지 않게 한다.

---

# 5. 설계 단점

- 핵심 write path가 Oracle T2에 집중되므로 DB connection, redo, unique index, account row lock이 최종 병목이다.
- API 앞 Waiting Room은 청약 성공을 보장하지 않으므로 사용자 안내가 필요하다.
- Oracle AQ/CDC/Kafka 경로는 lag, connector/bridge 장애, log retention을 운영 리스크로 가진다.
- 고정 pool은 안정적이지만 유휴 비용이 크고, autoscaling은 LB 반응 속도와 graceful drain을 관리해야 한다.
- 실제 균등/비례/우대 배정 규칙이 들어오면 allocation rule과 audit 설계가 더 필요하다.
- 외부 은행 송금까지 환불 범위에 넣으면 내부 release와 외부 transfer를 분리한 Saga 상태가 추가로 필요하다.
- 가장 치명적인 설계 문제는 DB에 핵심 로직을 의존하는 것이다.
