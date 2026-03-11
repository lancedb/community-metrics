'use client'

import { signIn } from 'next-auth/react'

type SignInButtonProps = {
  callbackUrl: string
}

export function SignInButton({ callbackUrl }: SignInButtonProps) {
  return (
    <button
      type="button"
      className="w-full rounded-lg bg-[#d9532a] px-4 py-2 text-sm font-semibold text-white transition hover:bg-[#bf4723]"
      onClick={() => signIn('google', { callbackUrl })}
    >
      Sign in with Google
    </button>
  )
}
