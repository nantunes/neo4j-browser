// @ts-nocheck
import neo4j, {
  AuthToken,
  Config,
  Driver,
  Session,
  Result,
  Record as NeoRecord,
  Transaction,
  ServerInfo,
  SessionConfig,
  TransactionConfig,
  QueryResult,
  ResultSummary,
  Integer,
  int,
  types
} from 'neo4j-driver'
import { Client } from 'pg'
import websocket from 'websocket-stream'

// Helper to create Integer with transport-class
const makeInt = (val: any) => {
  const integer = int(val)
  ;(integer as any)['transport-class'] = 'Integer'
  return integer
}

// Helper to convert AGE agtype to Neo4j types
const convertAgtype = (val: any): any => {
  if (val === null || val === undefined) {
    return null
  }

  if (typeof val === 'string') {
    // Strip AGE type suffixes like ::vertex, ::edge globally, but ignore those inside quotes
    // This handles nested objects where the inner object has a type annotation, e.g. {"n": {...}::vertex}
    const cleanVal = val.replace(/::\w+(?=(?:(?:[^"]*"){2})*[^"]*$)/g, '')
    try {
      const parsed = JSON.parse(cleanVal)
      // If parsing succeeds and results in a different value (or type), process it
      if (parsed !== val) {
        return convertAgtype(parsed)
      }
    } catch (e) {
      // Not JSON, continue
    }
    return val
  }

  if (typeof val === 'number' || typeof val === 'boolean') {
    return val
  }
  if (Array.isArray(val)) {
    return val.map(convertAgtype)
  }
  if (typeof val === 'object') {
    if (
      ('id' in val || 'identity' in val) &&
      ('label' in val || 'type' in val)
    ) {
      const id = val.id ?? val.identity
      const label = val.label ?? val.type
      const properties = val.properties || {}

      if (
        ('start_id' in val || 'start' in val) &&
        ('end_id' in val || 'end' in val)
      ) {
        // Relationship
        const start = val.start_id ?? val.start
        const end = val.end_id ?? val.end
        const rel = new types.Relationship(
          makeInt(id),
          makeInt(start),
          makeInt(end),
          label,
          properties
        )
        // Add transport-class for worker serialization
        ;(rel as any)['transport-class'] = 'Relationship'
        return rel
      } else {
        // Node
        const node = new types.Node(
          makeInt(id),
          Array.isArray(label) ? label : [label],
          properties
        )
        // Add transport-class for worker serialization
        ;(node as any)['transport-class'] = 'Node'
        return node
      }
    }

    const newObj: any = {}
    for (const key in val) {
      newObj[key] = convertAgtype(val[key])
    }
    return newObj
  }
  return val
}

class AgeResult implements Result {
  private _records: NeoRecord[]
  private _summary: ResultSummary
  private _promise: Promise<QueryResult>

  constructor(records: NeoRecord[], summary: ResultSummary) {
    this._records = records
    this._summary = summary
    this._promise = Promise.resolve({ records, summary })
  }

  async next(): Promise<IteratorResult<NeoRecord, any>> {
    const record = this._records.shift()
    if (record) {
      return { done: false, value: record }
    } else {
      return { done: true, value: null }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<NeoRecord, any, undefined> {
    return this
  }

  then<TResult1 = QueryResult, TResult2 = never>(
    onfulfilled?:
      | ((value: QueryResult) => TResult1 | PromiseLike<TResult1>)
      | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this._promise.then(onfulfilled, onrejected)
  }

  catch<TResult = never>(
    onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | null
  ): Promise<QueryResult | TResult> {
    return this._promise.catch(onrejected)
  }

  finally(onfinally?: (() => void) | null): Promise<QueryResult> {
    return this._promise.finally(onfinally)
  }

  isOpen(): boolean {
    return false
  }
}

class AgeTransaction implements Transaction {
  private client: Client
  private isOpenFlag: boolean = true
  private auth: AuthToken
  private graphName: string

  constructor(client: Client, auth: AuthToken, graphName: string) {
    this.client = client
    this.auth = auth
    this.graphName = graphName
  }

  async run(query: string, parameters?: any): Promise<{ result: Result }> {
    const graphName = this.graphName // || (this.client as any).defaultGraph

    console.log('[AgeDriver] Running query:', `${graphName}`, query)

    // Mocking Neo4j system queries
    const cleanQuery = query.trim()
    const upperQuery = cleanQuery.toUpperCase()

    const createMockResult = (records: any[]) => {
      return Promise.resolve({
        result: new AgeResult(records, {
          query: { text: query, parameters: parameters || {} },
          queryType: 'r',
          counters: {
            _stats: {
              nodesCreated: 0,
              nodesDeleted: 0,
              relationshipsCreated: 0,
              relationshipsDeleted: 0,
              propertiesSet: 0,
              labelsAdded: 0,
              labelsRemoved: 0,
              indexesAdded: 0,
              indexesRemoved: 0,
              constraintsAdded: 0,
              constraintsRemoved: 0
            }
          },
          updateStatistics: {
            containsUpdates: () => false,
            nodesCreated: () => 0,
            nodesDeleted: () => 0,
            relationshipsCreated: () => 0,
            relationshipsDeleted: () => 0,
            propertiesSet: () => 0,
            labelsAdded: () => 0,
            labelsRemoved: () => 0,
            indexesAdded: () => 0,
            indexesRemoved: () => 0,
            constraintsAdded: () => 0,
            constraintsRemoved: () => 0
          },
          plan: false,
          profile: false,
          notifications: [],
          server: {
            address: 'localhost:5432',
            version: 'Neo4j/4.4.0',
            protocolVersion: 4.4
          },
          resultConsumedAfter: int(0),
          resultAvailableAfter: int(0),
          database: { name: 'lineagedb' }
        })
      })
    }

    if (upperQuery === 'SHOW DATABASES') {
      try {
        const res = await this.client.query(
          'SELECT name FROM ag_catalog.ag_graph ORDER BY name'
        )

        const records = res.rows.map((row, index) => {
          return new NeoRecord(
            [
              'name',
              'address',
              'role',
              'requestedStatus',
              'currentStatus',
              'error',
              'default'
            ],
            [
              row.name,
              this.url, // Placeholder address
              'standalone',
              'online',
              'online',
              '',
              index === 0 // Make the first one default
            ]
          )
        })

        return createMockResult(records)
      } catch (e) {
        console.error('[AgeDriver] Failed to fetch databases:', e)
        return createMockResult([])
      }
    }

    if (upperQuery.includes('CALL DBMS.COMPONENTS()')) {
      return createMockResult([
        new NeoRecord(
          ['name', 'versions', 'edition'],
          ['Neo4j Kernel', ['4.0.0'], 'community']
        )
      ])
    }

    if (upperQuery.includes('CALL DBMS.CLIENTCONFIG()')) {
      return createMockResult([])
    }

    if (upperQuery.includes('CALL DBMS.SECURITY.SHOWCURRENTUSER()')) {
      return createMockResult([
        new NeoRecord(
          ['username', 'roles', 'flags'],
          [this.auth.principal, [], []]
        )
      ])
    }

    if (upperQuery.includes('CALL DBMS.SHOWCURRENTUSER()')) {
      return createMockResult([
        new NeoRecord(
          ['username', 'roles', 'flags'],
          [this.auth.principal, [], []]
        )
      ])
    }

    if (
      upperQuery.includes('CALL DBMS.PROCEDURES()') ||
      upperQuery.includes('SHOW PROCEDURES')
    ) {
      return createMockResult([
        new NeoRecord(
          ['name', 'description', 'signature'],
          ['apoc.help', 'Help', 'apoc.help(text)']
        )
      ])
    }

    if (
      upperQuery.includes('CALL DBMS.FUNCTIONS()') ||
      upperQuery.includes('SHOW FUNCTIONS')
    ) {
      return createMockResult([
        new NeoRecord(
          ['name', 'description', 'signature'],
          ['age.agtype', 'Returns agtype', 'age.agtype(any)']
        )
      ])
    }

    if (upperQuery.includes('CALL DBMS.LISTCONFIG()')) {
      return createMockResult([])
    }

    // Handle the combined metadata query from dbMetaDuck.ts
    if (
      upperQuery.includes('CALL DB.LABELS() YIELD LABEL') &&
      upperQuery.includes('UNION ALL')
    ) {
      const labels: string[] = []
      const relTypes: string[] = []

      try {
        const labelRes = await this.client.query(
          `SELECT ag_label.name FROM ag_catalog.ag_label JOIN ag_catalog.ag_graph ON ag_label.graph = ag_graph.graphid WHERE ag_label.kind = 'v' AND ag_label.name NOT IN ('_ag_label_vertex', '_ag_label_edge') AND ag_graph.name = '${graphName}'`
        )
        labels.push(...labelRes.rows.map(r => r.name))
      } catch (e) {
        console.error('[AgeDriver] Failed to fetch labels:', e)
      }

      try {
        const relRes = await this.client.query(
          `SELECT ag_label.name FROM ag_catalog.ag_label JOIN ag_catalog.ag_graph ON ag_label.graph = ag_graph.graphid WHERE ag_label.kind = 'e' AND ag_label.name NOT IN ('_ag_label_vertex', '_ag_label_edge') AND ag_graph.name = '${graphName}'`
        )
        relTypes.push(...relRes.rows.map(r => r.name))
      } catch (e) {
        console.error('[AgeDriver] Failed to fetch relationship types:', e)
      }

      // Construct the result in the format expected by dbMetaDuck.ts
      // It expects 3 rows, each with a 'result' column containing {name: string, data: any[]}
      const records = [
        new NeoRecord(['result'], [{ name: 'labels', data: labels }]),
        new NeoRecord(
          ['result'],
          [{ name: 'relationshipTypes', data: relTypes }]
        ),
        new NeoRecord(['result'], [{ name: 'propertyKeys', data: [] }]) // Property keys empty for now
      ]

      return createMockResult(records)
    }

    // Keep individual handlers just in case they are called separately (e.g. by user)
    if (upperQuery.includes('CALL DB.LABELS()')) {
      try {
        const res = await this.client.query(
          `SELECT ag_label.name FROM ag_catalog.ag_label JOIN ag_catalog.ag_graph ON ag_label.graph = ag_graph.graphid WHERE ag_label.kind = 'v' AND ag_label.name NOT IN ('_ag_label_vertex', '_ag_label_edge') AND ag_graph.name = '${graphName}'`
        )
        const records = res.rows.map(
          row => new NeoRecord(['label'], [row.name])
        )
        return createMockResult(records)
      } catch (e) {
        console.error('[AgeDriver] Failed to fetch labels:', e)
        return createMockResult([])
      }
    }

    if (upperQuery.includes('CALL DB.RELATIONSHIPTYPES()')) {
      try {
        const res = await this.client.query(
          `SELECT ag_label.name FROM ag_catalog.ag_label JOIN ag_catalog.ag_graph ON ag_label.graph = ag_graph.graphid WHERE ag_label.kind = 'e' AND ag_label.name NOT IN ('_ag_label_vertex', '_ag_label_edge') AND ag_graph.name = '${graphName}'`
        )
        const records = res.rows.map(
          row => new NeoRecord(['relationshipType'], [row.name])
        )
        return createMockResult(records)
      } catch (e) {
        console.error('[AgeDriver] Failed to fetch relationship types:', e)
        return createMockResult([])
      }
    }

    if (upperQuery.includes('CALL DB.PROPERTYKEYS()')) {
      // Property keys are not stored in a central catalog in AGE (they are in JSONB).
      // We could scan the graph, but that's expensive.
      // For now, let's return an empty list or maybe a static list if we knew the schema.
      return createMockResult([])
    }

    if (upperQuery.startsWith('SHOW PROCEDURES')) {
      return createMockResult([])
    }

    if (upperQuery.startsWith('SHOW FUNCTIONS')) {
      return createMockResult([])
    }

    if (upperQuery.startsWith('EXPLAIN') || upperQuery.startsWith('PROFILE')) {
      // AGE does not support EXPLAIN/PROFILE in the same way as Neo4j.
      // Return empty result to avoid syntax errors when clicking queries in browser.
      return createMockResult([])
    }
    if (!this.isOpenFlag) {
      throw new Error('Transaction is closed')
    }
    try {
      // Ensure search_path is set correctly
      // Handle connection errors - listeners are now attached in acquireClient

      try {
        await this.client.query('SET search_path = ag_catalog, "$user", public')
      } catch (e) {
        console.error('[AgeDriver] Failed to set search_path:', e)
        throw e
      }

      let sql = query
      const isCypher =
        /^\s*(MATCH|CREATE|MERGE|RETURN|WITH|UNWIND|CALL|LOAD|START|DROP|SET|DELETE|REMOVE|FOREACH)/i.test(
          query
        )

      let sqlParams: any[] = []

      if (isCypher) {
        if (!graphName) {
          throw new Error(
            'No graph selected and no default graph found in ag_catalog.ag_graph.'
          )
        }

        // Convert Neo4j types (like Integer) to native JS types for AGE
        const convertParameters = (val: any): any => {
          if (val === null || val === undefined) return val
          if (typeof val === 'object') {
            // Handle Neo4j Integer
            if (val.low !== undefined && val.high !== undefined) {
              return int(val).toNumber()
            }
            if (Array.isArray(val)) {
              return val.map(convertParameters)
            }
            const newObj: any = {}
            for (const k in val) {
              newObj[k] = convertParameters(val[k])
            }
            return newObj
          }
          return val
        }

        const cleanParams = convertParameters(parameters || {})

        // Pass parameters as a JSON string (agtype) to the cypher function
        // We use $1 as the placeholder for the parameters string
        // Also strip any trailing semicolon from the Cypher query
        let cleanQuery = query.trim().replace(/;$/, '')

        // Attempt to wrap return columns in a map to support multi-column results and preserve aliases
        // This converts "RETURN n, d" into "RETURN {n: n, d: d}" so it fits into the single 'v' return column
        // We only do this for simple identifier lists to avoid syntax errors with complex expressions
        if (/\bRETURN\s+(?!DISTINCT\s+)(?![{])/.test(cleanQuery)) {
          cleanQuery = cleanQuery.replace(
            /RETURN\s+(?!DISTINCT\s+)(?![{])(.+?)(?:\s+(?:ORDER\s+BY|LIMIT|SKIP|UNION)|$)/i,
            (match, p1, offset, string) => {
              // Check if p1 is a simple list of identifiers (no dots, parens, AS, etc)
              // We split by comma and check each part
              const parts = p1.split(',').map(s => s.trim())
              const allSimple = parts.every(part => /^\w+$/.test(part))

              if (allSimple) {
                const mapBody = parts.map(part => `${part}: ${part}`).join(', ')
                return match.replace(p1, `{${mapBody}}`)
              }

              // Fallback: if we can't safely rewrite, return original.
              return match
            }
          )
        }

        // Handle ID list checks (VisualizationView)
        // Relaxed regex to handle spaces and potential variations
        if (
          cleanQuery.match(
            /MATCH \(a\)-\[r\]->\(b\) WHERE id\(a\) IN \$existingNodeIds AND id\(b\) IN \$newNodeIds RETURN\s*\{\s*r:\s*r\s*\}/
          )
        ) {
          // We don't need toInteger() anymore since we converted parameters to numbers
          cleanQuery = `MATCH (a)-[r]->(b) WHERE id(a) IN $existingNodeIds AND id(b) IN $newNodeIds RETURN {r: r}`
        }

        sql = `SELECT * FROM ag_catalog.cypher('${graphName}', $$ ${cleanQuery} $$, $1::agtype) as (v agtype);`
        sqlParams = [JSON.stringify(cleanParams)]
      } else {
        // For raw SQL, we can't easily map named parameters to positional $1, $2...
        // So we assume the user isn't using parameters or is using them correctly for pg
        // But since this is a Neo4j driver, we should probably warn or try to handle it.
        // For now, let's stick to the previous behavior for non-Cypher queries (which likely fail with params anyway)
        sqlParams = Object.values(parameters || {})
      }

      let res
      try {
        res = await this.client.query(sql, sqlParams)
      } catch (e) {
        console.error('[AgeDriver] Query execution failed:', e)
        if (e instanceof Error) {
          console.error('[AgeDriver] Error details:', e.message, e.stack)
        }
        throw e
      }

      // Handle multi-statement results (pg returns an array) - though we shouldn't have them now with separate SET search_path
      const queryResult = Array.isArray(res) ? res[res.length - 1] : res

      const records = queryResult.rows.map(row => {
        let keys = Object.keys(row)
        let fields = Object.values(row).map(convertAgtype)

        // If we have a single column 'v' that is a map, it might be our wrapped result.
        // We unwrap it to restore the original columns (e.g. 'n', 'd').
        if (
          keys.length === 1 &&
          keys[0] === 'v' &&
          !Array.isArray(fields[0]) &&
          typeof fields[0] === 'object' &&
          fields[0] !== null
        ) {
          const val = fields[0]
          // Check if it's a graph entity (Node/Rel). If so, keep it as is (it's just a single return value).
          // If it's a generic map, we assume it's the wrapped projection.
          const isEntity =
            val instanceof types.Node || val instanceof types.Relationship

          if (!isEntity) {
            keys = Object.keys(val)
            fields = Object.values(val)
          }
        }

        const fieldLookup: Record<string, number> = {}
        keys.forEach((key, index) => {
          fieldLookup[key] = index
        })

        return new (types.Record as any)(keys, fields, fieldLookup)
      })

      const summary: ResultSummary = {
        query: { text: query, parameters: parameters || {} },
        queryType: 'r',
        counters: {
          _stats: {
            nodesCreated: 0,
            nodesDeleted: 0,
            relationshipsCreated: 0,
            relationshipsDeleted: 0,
            propertiesSet: 0,
            labelsAdded: 0,
            labelsRemoved: 0,
            indexesAdded: 0,
            indexesRemoved: 0,
            constraintsAdded: 0,
            constraintsRemoved: 0,
            systemUpdates: 0,
            containsSystemUpdates: false
          },
          containsUpdates: false,
          nodesCreated: 0,
          nodesDeleted: 0,
          relationshipsCreated: 0,
          relationshipsDeleted: 0,
          propertiesSet: 0,
          labelsAdded: 0,
          labelsRemoved: 0,
          indexesAdded: 0,
          indexesRemoved: 0,
          constraintsAdded: 0,
          constraintsRemoved: 0,
          systemUpdates: 0,
          containsSystemUpdates: false,
          updates: {}
        } as any,
        updateStatistics: {} as any,
        plan: false,
        profile: false,
        notifications: [],
        server: {
          address: 'apache-age',
          version: 'Age 1.0',
          protocolVersion: 1.0,
          agent: 'Neo4j Browser Age Adapter'
        },
        resultConsumedAfter: int(0),
        resultAvailableAfter: int(0),
        database: graphName
      }

      return { result: new AgeResult(records, summary) }
    } catch (e) {
      throw e
    }
  }

  async commit(): Promise<void> {
    await this.client.query('COMMIT')
    this.isOpenFlag = false
  }

  async rollback(): Promise<void> {
    await this.client.query('ROLLBACK')
    this.isOpenFlag = false
  }

  async close(): Promise<void> {
    if (this.isOpenFlag) {
      await this.rollback()
    }
  }

  isOpen(): boolean {
    return this.isOpenFlag
  }
}

class AgeSession implements Session {
  private clientPromise: Promise<Client>
  private config: SessionConfig
  private auth: AuthToken
  private releaseClient: (client: Client) => void

  constructor(
    clientPromise: Promise<Client>,
    config: SessionConfig,
    auth: AuthToken,
    releaseClient: (client: Client) => void
  ) {
    this.clientPromise = clientPromise
    this.config = config
    this.auth = auth
    this.releaseClient = releaseClient
  }

  run(query: string, parameters?: any, config?: TransactionConfig): Result {
    const resultPromise = this.clientPromise.then(client => {
      const tx = new AgeTransaction(client, this.auth, this.config?.database)
      return tx.run(query, parameters)
    })
    return new AgePendingResult(resultPromise)
  }

  beginTransaction(config?: TransactionConfig): Transaction {
    return new AgePendingTransaction(
      this.clientPromise,
      this.auth,
      this.config?.database
    )
  }

  lastBookmark(): string | null {
    return null
  }

  async close(): Promise<void> {
    try {
      const client = await this.clientPromise
      this.releaseClient(client)
    } catch (e) {
      // Ignore close errors if client failed to connect
    }
  }

  // @ts-ignore
  readTransaction<T>(
    work: (tx: Transaction) => Promise<T>,
    config?: TransactionConfig
  ): Promise<T> {
    return this.executeRead(work, config)
  }
  // @ts-ignore
  writeTransaction<T>(
    work: (tx: Transaction) => Promise<T>,
    config?: TransactionConfig
  ): Promise<T> {
    return this.executeWrite(work, config)
  }

  async executeRead<T>(
    work: (tx: Transaction) => Promise<T>,
    config?: TransactionConfig
  ): Promise<T> {
    const tx = this.beginTransaction(config)
    try {
      const result = await work(tx)
      await tx.commit()
      return result
    } catch (e) {
      await tx.rollback()
      throw e
    }
  }

  async executeWrite<T>(
    work: (tx: Transaction) => Promise<T>,
    config?: TransactionConfig
  ): Promise<T> {
    return this.executeRead(work, config)
  }

  lastBookmarks(): string[] {
    return []
  }
}

class AgePendingResult implements Result {
  private _wrapperPromise: Promise<{ result: Result }>

  constructor(wrapperPromise: Promise<{ result: Result }>) {
    this._wrapperPromise = wrapperPromise
  }

  async next(): Promise<IteratorResult<NeoRecord, any>> {
    const wrapper = await this._wrapperPromise
    // @ts-ignore
    return wrapper.result.next()
  }

  [Symbol.asyncIterator](): AsyncIterator<NeoRecord, any, undefined> {
    return this
  }

  then<TResult1 = QueryResult, TResult2 = never>(
    onfulfilled?:
      | ((value: QueryResult) => TResult1 | PromiseLike<TResult1>)
      | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this._wrapperPromise.then(wrapper => {
      // @ts-ignore
      return wrapper.result.then(onfulfilled, onrejected)
    })
  }

  catch<TResult = never>(
    onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | null
  ): Promise<QueryResult | TResult> {
    return this._wrapperPromise
      .then(wrapper => {
        // @ts-ignore
        return wrapper.result.catch(onrejected)
      })
      .catch(onrejected)
  }

  finally(onfinally?: (() => void) | null): Promise<QueryResult> {
    return this._wrapperPromise
      .then(wrapper => {
        // @ts-ignore
        return wrapper.result.finally(onfinally)
      })
      .catch(e => {
        if (onfinally) onfinally()
        throw e
      })
  }

  isOpen(): boolean {
    return false
  }
}

class AgePendingTransaction implements Transaction {
  private _clientPromise: Promise<Client>
  private _txPromise: Promise<Transaction>
  private auth: AuthToken
  private graphName: string

  constructor(
    clientPromise: Promise<Client>,
    auth: AuthToken,
    graphName: string
  ) {
    this._clientPromise = clientPromise
    this.auth = auth
    this.graphName = graphName
    this._txPromise = clientPromise.then(client => {
      client.query('BEGIN')
      return new AgeTransaction(client, auth, graphName)
    })
  }

  run(query: string, parameters?: any): Result {
    const resultPromise = this._txPromise.then(tx => tx.run(query, parameters))
    // @ts-ignore
    return new AgePendingResult(resultPromise)
  }

  async commit(): Promise<void> {
    const tx = await this._txPromise
    await tx.commit()
  }

  async rollback(): Promise<void> {
    const tx = await this._txPromise
    await tx.rollback()
  }

  async close(): Promise<void> {
    const tx = await this._txPromise
    await tx.close()
  }

  isOpen(): boolean {
    return true
  }
}

export class AgeDriver implements Driver {
  private url: string
  private auth: AuthToken
  private config: Config
  private pool: Client[] = []

  constructor(url: string, auth: AuthToken, config: Config) {
    this.url = url
    this.auth = auth
    this.config = config
  }

  private async acquireClient(dbName: string): Promise<Client> {
    // Try to find an idle client in the pool
    // Note: We assume all clients in the pool are connected to the same Postgres DB (dbName)
    // If dbName changes (which shouldn't happen for a single driver instance usually, but might if URL changes?),
    // we might need to handle that. But AgeDriver is per URL.

    // Check if we have a client in the pool
    while (this.pool.length > 0) {
      const client = this.pool.pop()
      // Check if client is still open/connected?
      // pg client doesn't have a simple 'isConnected' property exposed easily,
      // but we can check if the stream is writable.
      // @ts-ignore
      if (
        client &&
        client.connection &&
        client.connection.stream &&
        client.connection.stream.writable
      ) {
        return client
      }
      // If not connected, let it go (it will be GC'd)
    }

    // Create new client
    const urlObj = new URL(
      this.url
        .replace('age+ws://', 'http://')
        .replace('age+wss://', 'https://')
        .replace('bolt+ws://', 'http://')
        .replace('bolt+wss://', 'https://')
    )

    // Use self.location to support both Window and Web Worker contexts
    // @ts-ignore
    const globalLocation =
      typeof window !== 'undefined' ? window.location : self.location
    const proxyUrl = globalLocation.origin.replace(/^http/, 'ws') + '/age-proxy'
    const target = `${urlObj.hostname}:${parseInt(urlObj.port) || 5432}`
    const ws = websocket(`${proxyUrl}?target=${target}`)
    // @ts-ignore
    ws.setNoDelay = () => {}
    // @ts-ignore
    ws.setKeepAlive = () => {}
    // @ts-ignore
    ws.ref = () => {}
    // @ts-ignore
    ws.unref = () => {}
    // @ts-ignore
    ws.connect = () => {}

    const client = new Client({
      user: this.auth.principal,
      password: this.auth.credentials,
      database: dbName,
      stream: ws
    })

    // Handle connection errors globally for this client
    client.on('error', (err: any) => {
      console.error('[AgeDriver] Client error:', err)
      // Remove from pool if present
      const index = this.pool.indexOf(client)
      if (index > -1) {
        this.pool.splice(index, 1)
      }
      try {
        client.end()
      } catch {}
    })

    client.on('end', () => {
      console.warn('[AgeDriver] Client disconnected')
      // Remove from pool if present
      const index = this.pool.indexOf(client)
      if (index > -1) {
        this.pool.splice(index, 1)
      }
    })

    await client.connect()

    // Ensure AGE extension is created and loaded
    await client.query('CREATE EXTENSION IF NOT EXISTS age')
    await client.query("LOAD 'age'")
    await client.query('SET search_path = ag_catalog, "$user", public')

    // Discover default graph (first one in ag_catalog.ag_graph)
    const graphRes = await client.query(
      'SELECT name FROM ag_catalog.ag_graph ORDER BY name'
    )
    const availableGraphs = graphRes.rows.map(r => r.name)

    ;(client as any).defaultGraph = availableGraphs[0]

    return client
  }

  private releaseClient(client: Client) {
    // Put back in pool
    // @ts-ignore
    if (
      client &&
      client.connection &&
      client.connection.stream &&
      client.connection.stream.writable
    ) {
      this.pool.push(client)
    }
  }

  session(config?: SessionConfig): Session {
    const urlObj = new URL(
      this.url
        .replace('age+ws://', 'http://')
        .replace('age+wss://', 'https://')
        .replace('bolt+ws://', 'http://')
        .replace('bolt+wss://', 'https://')
    )
    const urlDb = urlObj.pathname.split('/')[1]

    // Connect to the database specified in the URL, or 'postgres' by default
    const dbName = urlDb || 'postgres'

    const connectPromise = this.acquireClient(dbName).catch(e => {
      if (
        e.message &&
        (e.message.includes('net.Socket') ||
          e.message.includes('not a constructor'))
      ) {
        throw new Error(
          "Direct connection to Postgres from browser is not supported. Please use 'age+ws://' scheme and run the local proxy ('yarn run proxy')."
        )
      }
      throw e
    })

    return new AgeSession(connectPromise, config, this.auth, client =>
      this.releaseClient(client)
    )
  }

  async close(): Promise<void> {}

  async verifyConnectivity(config?: {
    database?: string
  }): Promise<ServerInfo> {
    const session = this.session({ database: config?.database })
    try {
      await session.run('SELECT 1')
      return {
        address: this.url,
        version: 'Apache AGE',
        protocolVersion: 1.0,
        agent: 'Neo4j Browser Age Adapter'
      }
    } finally {
      await session.close()
    }
  }

  async supportsMultiDb(): Promise<boolean> {
    return true
  }

  async supportsTransactionConfig(): Promise<boolean> {
    return false
  }

  async supportsUserImpersonation(): Promise<boolean> {
    return false
  }

  isEncrypted(): boolean {
    return this.url.includes('+s')
  }

  async executeQuery<T>(
    query: string,
    parameters?: any,
    config?: any
  ): Promise<T> {
    const session = this.session(config)
    try {
      const result = await session.run(query, parameters)
      const queryResult = await result
      return queryResult as unknown as T
    } finally {
      await session.close()
    }
  }
}
