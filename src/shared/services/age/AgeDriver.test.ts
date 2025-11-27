import { AgeDriver } from './AgeDriver'
import { AuthToken, Config } from 'neo4j-driver'

// Mock pg Client
jest.mock('pg', () => {
  return {
    Client: jest.fn().mockImplementation(() => {
      return {
        connect: jest.fn().mockResolvedValue(undefined),
        query: jest.fn().mockImplementation((text, _params) => {
          if (text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK') {
            return Promise.resolve()
          }
          if (text === 'SELECT 1') {
            return Promise.resolve({ rows: [{ '?column?': 1 }] })
          }
          // Mock a cypher query result
          return Promise.resolve({
            rows: [
              {
                result: {
                  id: 1,
                  label: 'Person',
                  properties: { name: 'Alice' }
                }
              }
            ]
          })
        }),
        end: jest.fn().mockResolvedValue(undefined)
      }
    })
  }
})

describe('AgeDriver', () => {
  it('should connect and execute a query', async () => {
    const driver = new AgeDriver(
      'age+ws://localhost:5432',
      { principal: 'user', credentials: 'password' } as AuthToken,
      {} as Config
    )

    // Verify connectivity
    const serverInfo = await driver.verifyConnectivity()
    expect(serverInfo.address).toBe('age+ws://localhost:5432')

    // Execute query
    const result = (await driver.executeQuery(
      'SELECT * FROM cypher(...)'
    )) as any
    expect(result.records.length).toBe(1)
    const node = result.records[0].get('result')
    expect(node.properties.name).toBe('Alice')
    expect(node.labels).toEqual(['Person'])
  })
})
