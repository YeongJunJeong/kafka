# Kafka 주문 이벤트 Fan-out 실습 프로젝트

## 목표

사용자가 웹에서 "구매하기"를 누르면, 주문 정보가 Kafka로 발행되고,
6개의 Consumer가 각자 독립적으로 그 이벤트를 수신하는 것을 확인한다.

**이번 단계의 범위는 "배선 확인"까지다.**
재고 차감, 결제 처리 등 실제 비즈니스 로직은 구현하지 않는다.
각 Consumer는 수신한 메시지를 콘솔에 로그로 출력하기만 한다.

비유: 전선을 6군데로 다 빼놓고 전기가 들어오는지만 확인한다.
실제 전기 연결 공사(비즈니스 로직)는 다음 단계에서 한다.

## 아키텍처

```
사용자
  ↓ (구매하기 클릭)
React 웹 UI (localhost:3000)          ← 이번 단계에서는 선택사항
  ↓ POST /orders (JSON)
Spring Boot 주문 서비스 (localhost:8080)
  ↓ kafkaTemplate.send("order-created", ...)
Kafka broker (Docker, localhost:9092)
  ↓ 6개 group-id가 각자 독립적으로 pull
┌──────────┬──────────┬──────────┬──────────┬────────────┬────────────┐
│ 재고      │ 결제      │ 배송      │ 포인트    │ 구매자 알림  │ 판매자 알림  │
│ inventory│ payment  │ shipping │ points   │ buyer-noti │ seller-noti│
└──────────┴──────────┴──────────┴──────────┴────────────┴────────────┘
       (전부 System.out.println 으로 로그만 출력)
```

### 핵심 설계 원칙

- **토픽은 1개만 사용한다** (`order-created`). 서비스별로 토픽을 나누지 않는다.
- **각 Consumer는 서로 다른 group-id를 가진다.** group-id가 다르면 각 Consumer가
  독립적으로 전체 메시지를 모두 수신한다. group-id가 같으면 메시지를 나눠 갖게 되어
  의도한 fan-out 동작이 되지 않는다.
- 주문 서비스는 Kafka에 이벤트를 던지고 즉시 응답을 반환한다.
  Consumer들의 처리 완료를 기다리지 않는다.

## 환경

| 항목 | 값 |
|---|---|
| Java | 17 |
| Spring Boot | 4.1.0 |
| 빌드 도구 | Maven |
| 의존성 | Spring Web, Spring for Apache Kafka |
| Kafka | apache/kafka:latest (Docker, KRaft 모드, 단일 브로커) |
| Kafka 주소 | localhost:9092 |
| 앱 포트 | 8080 |

Kafka는 이미 Docker로 실행 중인 상태를 전제로 한다.
`docker-compose.yml` 위치: `C:\Users\USER\Project\Kafka\docker-compose.yml`

```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: Kafka
    ports:
      - "9092:9092"
```

## 프로젝트 구조

```
src/main/java/com/example/kafka/
├── KafkaApplication.java          # 기존 파일, 수정하지 않음
├── controller/
│   └── OrderController.java       # POST /orders 수신 → Kafka 발행
└── consumer/
    └── OrderListeners.java        # @KafkaListener 6개, 로그만 출력

src/main/resources/
└── application.properties         # Kafka 연결 설정
```

**주의:** 새로 만드는 패키지는 반드시 `KafkaApplication.java`가 위치한
`com.example.kafka` 하위에 두어야 한다. 스프링 부트는 메인 클래스 기준으로
하위 패키지만 컴포넌트 스캔하므로, 바깥에 두면 에러 없이 조용히 동작하지 않는다.

실제 생성된 패키지명이 `com.example.kafka`와 다르면 그에 맞춰 조정한다.

## 구현 계획

### 1단계: application.properties 설정

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.auto-offset-reset=earliest
```

- `auto-offset-reset=earliest`: 저장된 offset이 없는 새 group-id가 접속했을 때
  토픽의 처음부터 읽도록 한다. 앱을 나중에 켜도 이전에 발행된 메시지를 받을 수 있어
  실습 중 확인이 쉽다. (기본값 `latest`는 접속 이후 새 메시지만 받는다)

### 2단계: OrderController 구현

`com.example.kafka.controller.OrderController`

- `@RestController`
- `KafkaTemplate<String, String>`을 생성자 주입으로 받는다
- `@PostMapping("/orders")` 메서드:
  - `@RequestBody String order`로 JSON 문자열을 그대로 받는다
    (이번 단계에서는 DTO로 매핑하지 않고 문자열 그대로 처리한다)
  - `kafkaTemplate.send("order-created", order)` 호출
  - 즉시 "주문 접수 완료" 응답 반환

### 3단계: OrderListeners 구현

`com.example.kafka.consumer.OrderListeners`

- `@Component`
- `@KafkaListener` 메서드 6개, 각각 다른 group-id:

| 메서드 | groupId | 로그 프리픽스 |
|---|---|---|
| inventory | `inventory` | `[재고]` |
| payment | `payment` | `[결제]` |
| shipping | `shipping` | `[배송]` |
| points | `points` | `[포인트]` |
| buyerNoti | `buyer-noti` | `[구매자알림]` |
| sellerNoti | `seller-noti` | `[판매자알림]` |

- 모든 메서드가 `topics = "order-created"`를 구독한다
- 각 메서드는 `System.out.println(프리픽스 + " " + msg)` 만 수행한다
- **비즈니스 로직을 넣지 않는다**

## 실행 및 검증

### 실행 순서

1. Kafka 컨테이너 실행
   ```
   cd C:\Users\USER\Project\Kafka
   docker compose up -d
   docker ps          # Kafka 컨테이너가 Up 상태인지 확인
   ```

2. Spring Boot 앱 실행

3. 주문 요청 발생시키기
   ```
   curl -X POST http://localhost:8080/orders ^
     -H "Content-Type: application/json" ^
     -d "{\"orderId\":\"1001\",\"item\":\"laptop\",\"userId\":\"user01\"}"
   ```

### 성공 기준

콘솔에 아래 6줄이 모두 출력된다:

```
[재고] {"orderId":"1001","item":"laptop","userId":"user01"}
[결제] {"orderId":"1001","item":"laptop","userId":"user01"}
[배송] {"orderId":"1001","item":"laptop","userId":"user01"}
[포인트] {"orderId":"1001","item":"laptop","userId":"user01"}
[구매자알림] {"orderId":"1001","item":"laptop","userId":"user01"}
[판매자알림] {"orderId":"1001","item":"laptop","userId":"user01"}
```

**요청은 1번인데 6개가 모두 수신했다면 Kafka fan-out 검증 완료다.**

출력 순서는 매번 달라질 수 있다. 6개 Consumer가 서로 독립적으로 동작하므로
정상적인 현상이며, 순서를 맞추려 하지 않는다.

### 트러블슈팅

| 증상 | 원인 / 확인 사항 |
|---|---|
| 콘솔에 아무것도 안 뜸 | 패키지가 `com.example.kafka` 하위에 있는지 확인 (컴포넌트 스캔 범위) |
| 커넥션 에러 | `docker ps`로 Kafka 컨테이너가 실제로 Up 상태인지 확인 |
| 일부만 출력됨 | group-id가 6개 모두 서로 다른지 확인 (중복이면 메시지를 나눠 가짐) |
| `no configuration file provided` | `docker compose` 명령은 docker-compose.yml이 있는 폴더에서 실행해야 함 |

## 이번 단계에서 하지 않는 것

명시적으로 범위 밖이다. 요청받지 않는 한 구현하지 않는다.

- 실제 비즈니스 로직 (재고 차감, 결제 처리, 배송 등록, 포인트 적립, 알림 발송)
- DB 연동 / JPA
- DTO 클래스, JSON 역직렬화 (문자열 그대로 다룬다)
- 에러 처리, 재시도, Dead Letter Queue
- 멱등성 처리, 분산 트랜잭션
- Consumer를 별도 프로젝트로 분리 (지금은 단일 프로젝트 안에 전부 둔다)
- 인증/보안
- 테스트 코드

## 다음 단계 (참고용, 지금 하지 않음)

1. React UI 붙이기 — "구매하기" 버튼의 onClick에서 `POST /orders` 호출.
   백엔드 입장에서는 curl과 완전히 동일한 요청이므로 백엔드 수정은 불필요하다.
2. 각 Consumer의 `System.out.println` 자리에 실제 로직 채워넣기.
   Kafka 배선 자체는 바뀌지 않는다.
3. Consumer를 서비스별 독립 프로젝트로 분리 (실제 MSA 구조)
