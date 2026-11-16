import { redirect } from 'next/navigation'

import App from '@/App'
import { getAuthMode, isRequestAllowed } from '@/lib/auth'

export default async function Page() {
  if (!(await isRequestAllowed())) {
    redirect('/signin')
  }

  return <App showSignOut={getAuthMode() === 'google'} />
}
