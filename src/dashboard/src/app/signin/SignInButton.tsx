'use client'

import { signIn } from 'next-auth/react'

import { Button } from '@/components/ui/button'

type SignInButtonProps = {
  callbackUrl: string
}

export function SignInButton({ callbackUrl }: SignInButtonProps) {
  return (
    <Button type="button" size="lg" className="w-full" onClick={() => signIn('google', { callbackUrl })}>
      Sign in with Google
    </Button>
  )
}
