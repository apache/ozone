import { useState } from 'react'
import { Button } from 'antd'
import './App.css'

function App() {
  const [count, setCount] = useState(0)

  return (
    <div className="App">
      <h1>Ozone SCM</h1>
      <div className="card">
        <Button type="primary" onClick={() => setCount((count) => count + 1)}>
          Count is {count}
        </Button>
        <p>
          Edit <code>src/App.tsx</code> and save to test HMR
        </p>
      </div>
      <p className="read-the-docs">
        Click on the Vite and React logos to learn more
      </p>
    </div>
  )
}

export default App 