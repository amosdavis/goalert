/* eslint @typescript-eslint/no-var-requires: 0 */
/* eslint @typescript-eslint/no-require-imports: 0 */
const http = require('http')

// Call the Cypress-only Delta server. With a body, use POST; the fixture
// endpoints take plain SQL text and the clock endpoint takes JSON.
function makeDoCall(pathVal, base = 'http://127.0.0.1:3033') {
  return (body) =>
    new Promise((resolve, reject) => {
      const path = typeof pathVal === 'function' ? pathVal() : pathVal
      const isObject = body !== undefined && typeof body !== 'string'
      const payload =
        body === undefined ? null : isObject ? JSON.stringify(body) : body
      const req = http.request(
        base + path,
        {
          method: payload === null ? 'GET' : 'POST',
          headers:
            payload === null
              ? {}
              : {
                  'Content-Type': isObject
                    ? 'application/json'
                    : 'text/plain; charset=utf-8',
                },
        },
        (res) => {
          let data = ''
          res.on('data', (chunk) => {
            data += chunk
          })
          res.on('end', () => {
            if (res.statusCode < 200 || res.statusCode >= 300) {
              reject(
                new Error(
                  `request failed: ${res.statusCode}; url=${base + path}; ${data}`,
                ),
              )
              return
            }
            resolve(data)
          })
          res.on('error', reject)
        },
      )
      req.on('error', reject)
      req.end(payload === null ? undefined : payload)
    })
}

let expFlags = []
let apiOnly = false
function flagsQ() {
  const args = []
  if (expFlags.length > 0)
    args.push(`extra-arg=--experimental=${expFlags.join(',')}`)
  if (apiOnly) args.push('extra-arg=--api-only')
  return args.length ? '?' + args.join('&') : ''
}

let failed = false
module.exports = (on, config) => {
  async function engineCycle() {
    const cycleID = await makeDoCall('/health/engine/cycle', config.baseUrl)()
    await makeDoCall('/signal?sig=SIGUSR2')()
    await makeDoCall('/health/engine?id=' + cycleID, config.baseUrl)()
    return null
  }

  const stopBackend = makeDoCall('/stop')
  const startBackend = makeDoCall(() => '/start' + flagsQ())
  const resetStorage = makeDoCall('/reset')
  const fixture = makeDoCall('/fixture')
  const clock = makeDoCall('/clock')
  const resetClock = () => clock({ reset: true })

  on('task', {
    'engine:trigger': engineCycle,
    'db:reset': resetStorage,
    'db:fixture': fixture,
    'db:setTimeSpeed': (speed) => clock({ speed }),
    'db:fastforward': (advance) => clock({ advance }),
    'db:resettime': resetClock,
    'engine:setexpflags': (flags) => {
      expFlags = flags
      return null
    },
    'engine:setapionly': (val) => {
      apiOnly = val
      return null
    },
    'engine:start': startBackend,
    'engine:stop': stopBackend,
    'check:abort': () => failed,
  })

  on('before:spec', resetClock)

  on('after:spec', (spec, results) => {
    if (results.stats.failures) {
      failed = true
    }
  })
}
