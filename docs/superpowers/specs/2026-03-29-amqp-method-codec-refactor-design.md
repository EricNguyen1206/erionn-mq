# AMQP Method Codec Refactor Design

## Problem

`internal/amqp/methods.go` is 1037 lines implementing 31 AMQP 0-9-1 method types. It has three sources of duplication:

1. **Boilerplate interface methods** — `ClassID()` and `MethodID()` are 2-line stubs repeated for every type (62 lines total).
2. **Nested switch dispatch** — `DecodeMethodFrame` uses an 80-line nested switch over class ID then method ID.
3. **Identical wire layouts** — `ConnectionClose`/`ChannelClose` and `ConnectionTune`/`ConnectionTuneOk` share the same binary format but have separate marshal/decode implementations.

## Goals

- Reduce code without sacrificing type safety or performance.
- Make adding new AMQP methods easier (one map entry + embed header).
- Keep zero reflection, zero code generation, zero new dependencies.

## Design

### 1. Embedded `methodHeader` struct

Replace per-type `ClassID()`/`MethodID()` methods with a single embedded struct:

```go
// methodHeader carries the AMQP class and method IDs for a method type.
// The class and method values MUST match the concrete type they are embedded in
// (e.g. classConnection/10 for ConnectionStart). The type system cannot enforce
// this — a mismatched header will produce incorrect wire output.
type methodHeader struct {
    class  uint16
    method uint16
}

func (h methodHeader) ClassID() uint16  { return h.class }
func (h methodHeader) MethodID() uint16 { return h.method }
```

Every method type embeds `methodHeader` as its first field. Example:

```go
type ConnectionStart struct {
    methodHeader
    VersionMajor     byte
    VersionMinor     byte
    ServerProperties Table
    Mechanisms       string
    Locales          string
}
```

The `Method` interface remains unchanged (`ClassID()`, `MethodID()`, `marshal(*bytes.Buffer) error`). The embedding satisfies the interface automatically.

**All constructor sites that build method values must include the `methodHeader` field.** This applies everywhere a method struct is instantiated — both inside `methods.go` (decoder functions) and in all other files that construct methods for encoding. For example:

```go
c.sendMethod(0, ConnectionStart{
    methodHeader: methodHeader{classConnection, 10},
    VersionMajor: 0,
    VersionMinor: 9,
    // ...
})
```

**Caveat: empty Ok types are no longer zero-value.** Types like `ConnectionCloseOk{}`, `ExchangeDeclareOk{}`, etc. currently have no fields and can be constructed as bare `{}`. After embedding `methodHeader`, bare `{}` produces `methodHeader{0, 0}`, which encodes wrong class/method IDs to the wire. Every construction site — including `reflect.DeepEqual` comparisons in tests — must explicitly set the header. Type switches (`case ConnectionCloseOk:`) are unaffected since they match on the concrete type, not the header values.

**Caveat: the type system cannot prevent a mismatched header.** Setting `methodHeader{classConnection, 10}` on a `ChannelClose` compiles fine but produces incorrect wire output. The godoc comment on `methodHeader` warns about this. The test `TestServerConstructedMethods_HaveCorrectClassMethodIDs` catches mismatches by verifying the encoded bytes match expected class/method pairs.

### 2. Map-based decode dispatch

Replace the nested switch in `DecodeMethodFrame` with a flat map:

```go
type methodKey uint32

func mkKey(classID, methodID uint16) methodKey {
    return methodKey(uint32(classID)<<16 | uint32(methodID))
}

var methodDecoders = map[methodKey]func(*bytes.Reader) (Method, error){
    mkKey(classConnection, 10): decodeConnectionStart,
    mkKey(classConnection, 11): decodeConnectionStartOk,
    mkKey(classConnection, 30): decodeConnectionTune,
    mkKey(classConnection, 31): decodeConnectionTuneOk,
    mkKey(classConnection, 40): decodeConnectionOpen,
    mkKey(classConnection, 41): decodeConnectionOpenOk,
    mkKey(classConnection, 50): decodeConnectionClose,
    mkKey(classChannel, 10):    decodeChannelOpen,
    mkKey(classChannel, 11):    decodeChannelOpenOk,
    mkKey(classChannel, 40):    decodeChannelClose,
    mkKey(classExchange, 10):   decodeExchangeDeclare,
    mkKey(classQueue, 10):      decodeQueueDeclare,
    mkKey(classQueue, 11):      decodeQueueDeclareOk,
    mkKey(classQueue, 20):      decodeQueueBind,
    mkKey(classBasic, 10):      decodeBasicQos,
    mkKey(classBasic, 20):      decodeBasicConsume,
    mkKey(classBasic, 21):      decodeBasicConsumeOk,
    mkKey(classBasic, 30):      decodeBasicCancel,
    mkKey(classBasic, 31):      decodeBasicCancelOk,
    mkKey(classBasic, 40):      decodeBasicPublish,
    mkKey(classBasic, 60):      decodeBasicDeliver,
    mkKey(classBasic, 80):      decodeBasicAck,
    mkKey(classBasic, 90):      decodeBasicReject,
    mkKey(classBasic, 120):     decodeBasicNack,
    mkKey(classConfirm, 10):    decodeConfirmSelect,
}
```

For empty `Ok` types that carry no payload, use one-liner closures that explicitly set `methodHeader`. These are entries in the **same `methodDecoders` map** shown above — they are listed separately here only for readability. **Do not use reflection or generics for these.** A zero-value `methodHeader{0, 0}` would cause `EncodeMethodFrame` to write incorrect class/method IDs to the wire, so every closure must set the header explicitly:

```go
mkKey(classConnection, 51): func(*bytes.Reader) (Method, error) {
    return ConnectionCloseOk{methodHeader: methodHeader{classConnection, 51}}, nil
},
mkKey(classChannel, 41): func(*bytes.Reader) (Method, error) {
    return ChannelCloseOk{methodHeader: methodHeader{classChannel, 41}}, nil
},
mkKey(classExchange, 11): func(*bytes.Reader) (Method, error) {
    return ExchangeDeclareOk{methodHeader: methodHeader{classExchange, 11}}, nil
},
mkKey(classQueue, 21): func(*bytes.Reader) (Method, error) {
    return QueueBindOk{methodHeader: methodHeader{classQueue, 21}}, nil
},
mkKey(classBasic, 11): func(*bytes.Reader) (Method, error) {
    return BasicQosOk{methodHeader: methodHeader{classBasic, 11}}, nil
},
mkKey(classConfirm, 11): func(*bytes.Reader) (Method, error) {
    return ConfirmSelectOk{methodHeader: methodHeader{classConfirm, 11}}, nil
},
```

The updated `DecodeMethodFrame`:

```go
func DecodeMethodFrame(frame Frame) (Method, error) {
    if frame.Type != FrameMethod {
        return nil, fmt.Errorf("amqp: expected method frame, got type %d", frame.Type)
    }
    r := bytes.NewReader(frame.Payload)
    var classID, methodID uint16
    if err := binary.Read(r, binary.BigEndian, &classID); err != nil {
        return nil, err
    }
    if err := binary.Read(r, binary.BigEndian, &methodID); err != nil {
        return nil, err
    }
    dec, ok := methodDecoders[mkKey(classID, methodID)]
    if !ok {
        return nil, fmt.Errorf("amqp: unsupported method %d.%d", classID, methodID)
    }
    return dec(r)
}
```

The map is read-only after initialization. No synchronization is needed.

### 3. Shared codec helpers

#### Close wire format (ConnectionClose / ChannelClose)

Both types encode: `uint16 replyCode`, `shortstr replyText`, `uint16 classRef`, `uint16 methodRef`.

```go
func marshalClose(w *bytes.Buffer, replyCode uint16, replyText string, classRef, methodRef uint16) error {
    binary.Write(w, binary.BigEndian, replyCode) //nolint:errcheck
    if err := writeShortstr(w, replyText); err != nil {
        return err
    }
    binary.Write(w, binary.BigEndian, classRef)  //nolint:errcheck
    binary.Write(w, binary.BigEndian, methodRef) //nolint:errcheck
    return nil
}

func decodeClose(r *bytes.Reader) (replyCode uint16, replyText string, classRef, methodRef uint16, err error) {
    if err = binary.Read(r, binary.BigEndian, &replyCode); err != nil {
        return
    }
    if replyText, err = readShortstr(r); err != nil {
        return
    }
    if err = binary.Read(r, binary.BigEndian, &classRef); err != nil {
        return
    }
    err = binary.Read(r, binary.BigEndian, &methodRef)
    return
}
```

Per-type wrappers:

```go
func (m ConnectionClose) marshal(w *bytes.Buffer) error {
    return marshalClose(w, m.ReplyCode, m.ReplyText, m.ClassIDRef, m.MethodIDRef)
}

func decodeConnectionClose(r *bytes.Reader) (Method, error) {
    rc, rt, cr, mr, err := decodeClose(r)
    if err != nil {
        return nil, err
    }
    return ConnectionClose{methodHeader: methodHeader{classConnection, 50}, ReplyCode: rc, ReplyText: rt, ClassIDRef: cr, MethodIDRef: mr}, nil
}
```

`ChannelClose` follows the same pattern with `methodHeader{classChannel, 40}`.

#### Tune wire format (ConnectionTune / ConnectionTuneOk)

Both types encode: `uint16 channelMax`, `uint32 frameMax`, `uint16 heartbeat`.

```go
func marshalTune(w *bytes.Buffer, channelMax uint16, frameMax uint32, heartbeat uint16) error {
    binary.Write(w, binary.BigEndian, channelMax) //nolint:errcheck
    binary.Write(w, binary.BigEndian, frameMax)   //nolint:errcheck
    binary.Write(w, binary.BigEndian, heartbeat)  //nolint:errcheck
    return nil
}

func decodeTune(r *bytes.Reader) (channelMax uint16, frameMax uint32, heartbeat uint16, err error) {
    if err = binary.Read(r, binary.BigEndian, &channelMax); err != nil {
        return
    }
    if err = binary.Read(r, binary.BigEndian, &frameMax); err != nil {
        return
    }
    err = binary.Read(r, binary.BigEndian, &heartbeat)
    return
}
```

Per-type wrappers delegate to these helpers, wrapping results with the correct `methodHeader`.

### 4. File organization

All changes stay in `internal/amqp/methods.go`. The file remains the single source for method type definitions, marshal/decode logic, and dispatch. No new files are created.

## What changes

| Component | Before | After |
|---|---|---|
| `ClassID()` / `MethodID()` | 62 lines across 31 types | Satisfied by `methodHeader` embedding |
| `DecodeMethodFrame` dispatch | 80-line nested switch | Map lookup + single error path |
| `ConnectionClose` / `ChannelClose` codec | 2 separate implementations | Shared `marshalClose` / `decodeClose` |
| `ConnectionTune` / `ConnectionTuneOk` codec | 2 separate implementations | Shared `marshalTune` / `decodeTune` |
| Empty Ok decoders | Inline in switch | One-liner closures in map |

## Files outside methods.go that need updating

Every site that constructs a method struct value must add `methodHeader`. These are the affected locations:

**`internal/amqp/connection.go`:**
- `startMethod()` — `ConnectionStart{...}`
- `handleMethod()` case `ConnectionClose` — `ConnectionCloseOk{}`
- `handleConnectionStartOk()` — `ConnectionTune{...}`
- `handleConnectionOpen()` — `ConnectionOpenOk{}`
- `sendDelivery()` — `BasicDeliver{...}`

**`internal/amqp/channel.go`:**
- `handleChannelOpen()` — `ChannelOpenOk{...}`
- `handleChannelClose()` — `ChannelCloseOk{}`

**`internal/amqp/topology.go`:**
- `handleExchangeDeclare()` — `ExchangeDeclareOk{}`
- `handleQueueDeclare()` — `QueueDeclareOk{...}`
- `handleQueueBind()` — `QueueBindOk{}`

**`internal/amqp/consume.go`:**
- `handleBasicQos()` — `BasicQosOk{}`
- `handleBasicConsume()` — `BasicConsumeOk{...}`
- `handleBasicCancel()` — `BasicCancelOk{...}`

**`internal/amqp/publish.go`:**
- `handleBasicPublish()` — `BasicNack{...}`, `BasicAck{...}` (confirm-mode error/success paths)
- `handleConfirmSelect()` — `ConfirmSelectOk{}`

**`internal/amqp/server_test.go`:**
- `handshake()` — `ConnectionStartOk{...}`, `ConnectionTuneOk{...}`, `ConnectionOpen{...}`
- `openChannel()` — `ChannelOpen{}`
- `consume()` — `BasicConsume{...}`
- `declareExchange()` — `ExchangeDeclare{...}`
- `declareQueue()` — `QueueDeclare{...}`
- `bindQueue()` — `QueueBind{...}`
- `cancelConsumer()` — `BasicCancel{...}`
- `publish()` — `BasicPublish{...}`
- `closeChannelAndConnection()` — `ChannelClose{...}`, `ConnectionClose{...}`
- Inline test bodies — `BasicAck`, `BasicNack`, `BasicReject`, `BasicQos`, `ConfirmSelect`

## Test updates

- **`TestMethodFrames_RoundTrip`**: All 9 test cases must include `methodHeader{...}` in their method values. `reflect.DeepEqual` works as before since `methodHeader` is a concrete embedded field.
- **New test: `TestDecodeMethodFrame_UnsupportedMethod`**: Verifies that an unknown class/method key returns `"amqp: unsupported method"` error.
- **New test: `TestMarshalClose_DecodeClose_RoundTrip`**: Directly tests `marshalClose`/`decodeClose` helpers with both `ConnectionClose` and `ChannelClose` values.
- **New test: `TestMarshalTune_DecodeTune_RoundTrip`**: Directly tests `marshalTune`/`decodeTune` helpers with both `ConnectionTune` and `ConnectionTuneOk` values.
- **New test: `TestMethodDecoders_AllMethodsRegistered`**: Iterates the map and verifies every entry is non-nil.
- **New test: `TestServerConstructedMethods_HaveCorrectClassMethodIDs`**: Encodes methods constructed by the server (e.g. `ConnectionStart`, `ConnectionTune`, `BasicDeliver`) via `EncodeMethodFrame` and verifies the class/method IDs on the wire are correct. This catches the case where a developer forgets to set `methodHeader`.

## Estimated impact

- Lines: ~1037 → ~980
- **Primary value**: reduced cognitive load when adding new AMQP methods — one map entry + embed header vs 6+ lines of boilerplate (ClassID, MethodID, switch case, decode wrapper). The ~57 line reduction is a secondary benefit.
- New test coverage for dispatch, shared helpers, and server-constructed method encoding
- Zero performance regression (map lookup vs switch is comparable for 31 sparse keys)
- Zero new dependencies

## Edge cases

- **Unknown class/method**: Single error path via map miss — identical behavior to current switch default.
- **Empty payload Ok types**: Decoder closures explicitly set `methodHeader`. No reader bytes consumed.
- **Missing methodHeader on constructed methods**: If a caller constructs a method without `methodHeader`, the encoder will write class=0, method=0 to the wire. The test `TestServerConstructedMethods_HaveCorrectClassMethodIDs` catches this.
- **Partial reads**: Shared helpers (`decodeClose`, `decodeTune`) propagate errors from individual `binary.Read` calls — same error behavior as before.
- **Concurrent map access**: Map is read-only after init. No synchronization needed.
