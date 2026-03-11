import { redirect } from 'next/navigation'
import { getServerSession } from 'next-auth'

import App from '@/App'
import { authOptions, isAllowedLanceDbEmail } from '@/lib/auth'

export default async function Page() {
  const session = await getServerSession(authOptions)
  if (!isAllowedLanceDbEmail(session?.user?.email)) {
    redirect('/signin')
  }

  return <App />
}
