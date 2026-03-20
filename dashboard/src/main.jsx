import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import SimDashboard from './SimDashboard'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <SimDashboard
      basePath={import.meta.env.VITE_BASE_PATH ?? ''}
      pollMs={parseInt(import.meta.env.VITE_POLL_MS ?? '2000')}
    />
  </StrictMode>
)