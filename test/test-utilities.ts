import {crc32c} from "@node-rs/crc32";
import Long from "long"
import {strictEqual} from "node:assert/strict"
import {before, after, test} from "node:test"

import {initClient} from "../src/init-client"
import {loadShared} from "../src/load-shared"


test("setup", async (ctx) => {

  const sql = initClient()

  before(async () => {
    await loadShared(sql)
  })

  after(async () => {
    await sql.end()
  })

  await ctx.test("sorted json", async () => {
    type UnknownObject = Record<string, unknown>

    const sortUnknown = (inValue: unknown): unknown => {
      if (inValue !== null && typeof inValue === "object") {
        if (Array.isArray(inValue)) {
          return inValue.map(sortUnknown)
        } else {
          return sortObject(inValue as UnknownObject)
        }
      }
      return inValue
    }

    const sortObject = <T extends UnknownObject>(inObject: T): T => {
      const newObject: UnknownObject = {}
      for (const key of Object.keys(inObject).sort()) {
        newObject[key] = sortUnknown(inObject[key])
      }
      return newObject as T
    }

    const stmt = (json: any) => sql`SELECT evently._sorted_json(${sql.json(json)}) AS sorted_json`

    const compareSorting = async (input: any, msg: string) => {
      const expected = JSON.stringify(sortUnknown(input))
      const [{sorted_json}] = await stmt(input)
      strictEqual(sorted_json, expected, msg)
    }

    await compareSorting("hi", "sorted string matches")
    await compareSorting(true, "sorted boolean matches")
    await compareSorting(-223.554, "sorted number matches")
    await compareSorting([1, 2, 3], "sorted array matches")
    await compareSorting([1, "berries", false, null, ["a","b"]], "sorted mixed array matches")
    await compareSorting({b: 1, "🌲": "tree", aa: 2}, "sorted object matches")
    await compareSorting({d: 1, a: {aa: "hello", bb: {d: "oranges", ccc: [true, false]}}, b: null}, "sorted nested object matches")
  })

  await ctx.test("parse event id", async (t) => {
    await t.test("generated event id", async () => {
      const expected = [Date.now(), Long.fromInt(9574744).toUnsigned(), "abcd1234"]
      // create a uuid string from expected
      const eidParts = [
        expected[0].toString(16).padStart(16, "0"),
        expected[1].toString(16).padStart(8, "0"),
        expected[2]
      ]
      const eid = eidParts.join("")
      // UUID canonical form: 8-4-4-4-12 (36 chars)
      const uuidParts = [
        eid.substring(0, 8),
        eid.substring(8, 12),
        eid.substring(12, 16),
        eid.substring(16, 20),
        eid.substring(20)
      ]
      const uuid = uuidParts.join("-")

      const [{actual}] = await sql`SELECT evently._parse_event_id(${uuid}::UUID) AS actual`
      strictEqual(actual, `(${expected.join(",")})`)
    })
  })

  await ctx.test("crc32c", async (t) => {
    const stmt = (crc: number, input: string) => sql`SELECT evently._calc_crc32c(${crc}, ${input}) AS crc_value`

    await t.test("crc32c single", async () => {
      const expected = BigInt(crc32c("testing"))
      const [{ crc_value }] = await stmt(0, "testing")
      strictEqual(crc_value, expected, "single crc32c matches")
    })

    await t.test("crc32c multiple", async () => {
      const values = ["a", "list", "of","📒𒍑", "554456", new Date().toString()]
      const expected = values
        .reduce((crc, value) => crc32c(value, crc), 0)

      let actual = 0
      for (const value of values) {
        const [{ crc_value }] = await stmt(actual, value)
        actual = crc_value
      }
      strictEqual(actual, BigInt(expected), "multiple crc32c matches")
    })

    await t.test("crc32c big string", async (t2) => {
      const values = ["one", "{two}", "3", new Date().toString()]
      const bigValue = values.join("")

      await t2.test("in JS", async () => {
        const expected = values
          .reduce((crc, value) => crc32c(value, crc), 0)

        const actual = crc32c(bigValue)
        strictEqual(expected, actual)
      })

      await t2.test("in Postgres", async () => {
        let expected = 0
        for (const value of values) {
          const [{ crc_value }] = await stmt(expected, value)
          expected = crc_value
        }

        let actual = 0
        const [{ crc_value }] = await stmt(actual, bigValue)
        actual = crc_value
        strictEqual(expected, actual)
      })
    })
  })
})
