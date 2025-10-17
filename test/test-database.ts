import {randomUUID} from "crypto"
import {doesNotReject, rejects, strictEqual} from "node:assert/strict"
import {after, before, test} from "node:test"
import {Sql} from "postgres"

import { initClient } from "../src/init-client"
import { loadAll } from "../src/load-all"


// shared across multiple tests
const thingKey = "thing1"
const thingCreatedEvent = "thing-created"
const thingDeletedEvent = "thing-deleted"

const homeTableKey = "home"
const workTableKey = "work"
const pingEvent = "ball-pinged"
const pongEvent = "ball-ponged"
const falseSelector = Buffer.from("false")


async function shutdownDb(db: Sql) {
  await db.end()
}


type EventId = {
  timestamp:  number,
  checksum:   number,
  ledgerId:   string
}


function eventIdToUuid(eventId: EventId) {
  const {timestamp, checksum, ledgerId} = eventId
  const ts = timestamp.toString(16).padStart(16, "0")
  const chk = checksum.toString(16).padStart(8, "0")
  // UUID canonical form: 8-4-4-4-12 (36 chars)
  const parts = [
    ts.substring(0, 8),
    ts.substring(8, 12),
    ts.substring(12),
    chk.substring(0, 4),
    chk.substring(4) + ledgerId
  ]
  return parts.join("-")
}

test("setup", async (ctx) => {
  const sql = initClient()

  before(async () => await loadAll(sql))

  after(async () => await shutdownDb(sql))


  const createLedgerStmt = async (
      name:         string | null,
      description:  string | null): Promise<string> => {
    const [{ledger_id}] = await sql`SELECT evently.create_ledger(${name}, ${description}) AS ledger_id`
    return ledger_id
  }

  const ledger = "unit-tests"
  const desc = "unit-testing ledger"

  const testLedgerId = await createLedgerStmt(ledger, desc)

  const [{checksum, timestamp}] = await sql`SELECT timestamp, checksum FROM evently.ledger_base`
  const genesisEventId = {
    timestamp,
    checksum,
    ledgerId: testLedgerId
  }

  await ctx.test("ledgers", async (t) => {

    await t.test("cannot insert null ledger name", async () => {
      await rejects(
        createLedgerStmt(null, desc),
        /null value in column "name" of relation "ledgers" violates not-null constraint/,
        "cannot insert null name")
    })

    await t.test("cannot insert null description", async () => {
      await rejects(
        createLedgerStmt(ledger, null),
        /null value in column "description" of relation "ledgers" violates not-null constraint/,
        "cannot insert null description")
    })

    await t.test("cannot insert duplicate ledger", async () => {
      await rejects(
        createLedgerStmt(ledger, desc),
        /duplicate key value violates unique constraint "ledgers_name_key"/,
        "cannot insert duplicate ledger")
    })

    await t.test("Can use quotes in ledger name and description", async () => {
      await doesNotReject(createLedgerStmt(`a"test`, `A "test" ledger`))
    })
  })

  await ctx.test("events", async (t) => {
    let thingEventId1: EventId
    let thingEventId2: EventId
    let pingEventHomeId: EventId
    let pingEventWorkId: EventId

    const testEntities = {aThing: ["aThingKey"]}
    const testMeta = {actor: "unit-tests"}
    const testData = {}

    const appendStmt = async (
                              event:      string | null,
                              entities:   any,
                              meta:       any,
                              data:       any,
                              appendKey:  string | null,
                              afterEvent: EventId | null,
                              selector:   Buffer | null): Promise<EventId> => {
      // all these null types and checks are for the tests to pass invalid values
      const afterEventUuid = afterEvent ? eventIdToUuid(afterEvent) : null
      const [{event_id}] = await sql`SELECT evently.append_event(
       ${afterEventUuid}::UUID,
       ${event}::TEXT,
       ${sql.json(entities)}::JSONB,
       ${sql.json(meta)}::JSONB,
       ${sql.json(data)}::JSONB,
       ${appendKey}::TEXT,
       ${selector}::BYTEA) AS event_id`

      return parseEventId(event_id)
    }

    const insertStmt = async (
                              ledgerId:   string | null,
                              timestamp:  number | null,
                              previousTs: number | null,
                              checksum:   number | null,
                              appendKey:  string | null,
                              event:      string | null,
                              entities:   any | null,
                              meta:       any | null,
                              data:       any | null) => {
      const [{ledger_table}] = await sql`SELECT evently._ledger_table(${ledgerId}) AS ledger_table`
      await sql`INSERT INTO ${sql.unsafe(ledger_table)} (timestamp, previous_ts, checksum, append_key, event, entities, meta, data)
                VALUES (${timestamp}, ${previousTs}, ${checksum}, ${appendKey}, ${event}, ${sql.json(entities)}, ${sql.json(meta)}, ${sql.json(data)})`
    }


    await t.test("INSERT events", async (t) => {
      const timestamp = Date.now() * 1_000
      const appendKey = randomUUID()
      const previousTs = genesisEventId.timestamp

      await t.test("cannot insert null values", async () => {
        await rejects(
          insertStmt(testLedgerId, null, previousTs, checksum, appendKey, thingCreatedEvent, testEntities, testMeta, testData),
          /null value in column "timestamp" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null timestamp")
        await rejects(
          insertStmt(testLedgerId, timestamp, previousTs, null, appendKey, thingCreatedEvent, testEntities, testMeta, testData),
          /null value in column "checksum" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null checksum")
        await rejects(
          insertStmt(testLedgerId, timestamp, null, checksum, appendKey, thingCreatedEvent, testEntities, testMeta, testData),
          /null value in column "previous_ts" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null previous_ts")
        await rejects(
          insertStmt(testLedgerId, timestamp, previousTs, checksum, null, thingCreatedEvent, testEntities, testMeta, testData),
          /null value in column "append_key" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null append_key")
        await rejects(
          insertStmt(testLedgerId, timestamp, previousTs, checksum, appendKey, null, testEntities, testMeta, testData),
          /null value in column "event" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null event")
        await rejects(
          insertStmt(testLedgerId, timestamp, previousTs, checksum, appendKey, thingCreatedEvent, null, testMeta, testData),
          /null value in column "entities" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null entities")
        await rejects(
          insertStmt(testLedgerId, timestamp, previousTs, checksum, appendKey, thingCreatedEvent, testEntities, null, testData),
          /null value in column "meta" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null meta")
        await rejects(
          insertStmt(testLedgerId, timestamp, previousTs, checksum, appendKey, thingCreatedEvent, testEntities, testMeta, null),
          /null value in column "data" of relation "ledger_(.+)" violates not-null constraint/,
          "cannot insert null data")
      })


      await t.test("Cannot insert event into wrong ledger", async () => {
        await rejects(
          insertStmt("notanlid", timestamp, previousTs, checksum, appendKey, thingCreatedEvent, testEntities, testMeta, testData),
          /relation "ledger_notanlid" does not exist/,
          "cannot insert non-existent ledger")
      })

      const thingEvent1AppendKey = randomUUID()

      thingEventId1 = await appendStmt(thingCreatedEvent, testEntities, testMeta, {thingKey}, thingEvent1AppendKey, genesisEventId, falseSelector)
      thingEventId2 = await appendStmt(thingDeletedEvent, testEntities, testMeta, {thingKey}, randomUUID(), thingEventId1, falseSelector)
      pingEventHomeId = await appendStmt(pingEvent, testEntities, testMeta, {homeTableKey}, randomUUID(), thingEventId2, falseSelector)
      pingEventWorkId = await appendStmt(pingEvent, testEntities, testMeta, {workTableKey}, randomUUID(), pingEventHomeId, falseSelector)

      await t.test("append_key rules", async () => {
        await rejects(
          insertStmt(testLedgerId, timestamp, pingEventWorkId.timestamp, checksum, thingEvent1AppendKey, thingDeletedEvent, testEntities, testMeta, testData),
          /duplicate key value violates unique constraint "ledger_(.*)_append_key_key"/,
          "cannot insert different event with same append_key")
      })

      await t.test("previous_ts rules", async () => {
        await rejects(
          appendStmt(pongEvent, testEntities, testMeta, testData, randomUUID(), {
              timestamp: 1,
              checksum,
              ledgerId: testLedgerId
            }, falseSelector),
          /AFTER not found/,
          "cannot insert non-existent previous_ts")

        await rejects(
          insertStmt(testLedgerId, timestamp, thingEventId1.timestamp, checksum, randomUUID(), thingDeletedEvent, testEntities, testMeta, testData),
          /duplicate key value violates unique constraint "ledger_(.*)_previous_ts_key"/,
          "cannot insert different event for same previous")
      })

      await t.test("append_event()", async (t) => {
        const appendKey = randomUUID()

        await t.test("cannot send invalid attributes", async () => {
          await rejects(
            appendStmt(null, testEntities, testMeta, testData, appendKey, thingEventId2, falseSelector),
            /null value in column "event" of relation "ledger_(.*)" violates not-null constraint/,
            "cannot send null event")

          await rejects(
            appendStmt(thingCreatedEvent, null, testMeta, testData, appendKey, thingEventId2, falseSelector),
            /null value in column "entities" of relation "ledger_(.*)" violates not-null constraint/,
            "cannot send null entities")

          await rejects(
            appendStmt(thingCreatedEvent, testEntities, testMeta, testData, null, thingEventId2, falseSelector),
            /null value in column "append_key" of relation "ledger_(.*)" violates not-null constraint/,
            "cannot send null append key")

          await rejects(
            appendStmt(thingCreatedEvent, testEntities, testMeta, testData, appendKey, null, falseSelector),
            /null values cannot be formatted as an SQL identifier/,
            "cannot send null after event")

          await rejects(
            appendStmt(thingCreatedEvent, testEntities, testMeta, testData, appendKey, thingEventId2, null),
            /selector cannot be empty/,
            "cannot send null selector")
        })

        await t.test("cannot race appending to a ledger", async () => {
          const runQuery = async (): Promise<string> => {
            const selectorStmt = Buffer.from(`data @? '$.only ? (@ == 1)'`, "utf8")
            try {
              await appendStmt(pingEvent, testEntities, testMeta, {only:1}, randomUUID(), pingEventWorkId, selectorStmt)
              return "success"
            } catch (e: any) {
              if (e.message.startsWith("RACE")) {
                return "race"
              } else {
                throw e
              }
            }
          }
          const appends = []
          for (let i = 0; i < 100; i++) {
            appends.push(runQuery())
          }
          const results = await Promise.all(appends)
          const successes = results.filter(result => "success" === result)
          strictEqual(successes.length, 1, "only one success in race")
        })

        await t.test("cannot append if a selector has results", async () => {
          const selectorStmt = Buffer.from(`meta @? '$.actor ? (@ == "unit-tests")'`, "utf8")
          await rejects(
            appendStmt(thingCreatedEvent, testEntities, testMeta, testData, randomUUID(), genesisEventId, selectorStmt),
            /RACE CONDITION after mark/,
            "cannot append with selector that has results"
          )
        })
      })
    })
  })

  await ctx.test("update/delete constraints", async (t) => {
    await t.test("on ledger_base", async (t) => {
      await t.test("cannot UPDATE events from base_ledger", async () => {
        await rejects(sql`UPDATE evently.ledger_base SET meta = '"FAIL"'::JSONB`,
          /permission denied for table ledger_base/,
          "Should not be able to UPDATE events")
      })

      await t.test("cannot DELETE events in base_ledger", async () => {
        await rejects(sql`DELETE FROM evently.ledger_base`,
          /permission denied for table ledger_base/,
          "Should not be able to DELETE events")
      })
    })

    await t.test("on specific ledger", async (t) => {
      const [{ledger_table: ledgerTable}] = await sql`SELECT evently._ledger_table(${testLedgerId}) AS ledger_table`

      await t.test("cannot UPDATE events from a ledger", async () => {
        await rejects(sql`UPDATE ${sql.unsafe(ledgerTable)} SET meta = '"FAIL"'::JSONB`,
        /permission denied for table ledger_/,
        "Should not be able to UPDATE events")
      })
    })
  })

  await ctx.test("remove ledger", async () => {
    await doesNotReject(sql`SELECT evently.remove_ledger(${testLedgerId})`,
    "Should be able to remove a ledger")
  })
})

function parseEventId(eventId: string): EventId {
  return {
    timestamp: parseInt(eventId.substring(0, 16), 16),
    checksum:  parseInt(eventId.substring(16, 24), 16),
    ledgerId:  eventId.substring(24)
  }
}
