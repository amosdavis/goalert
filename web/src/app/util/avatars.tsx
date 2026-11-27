import React from 'react'
import { Layers, RotateRight, Today, VpnKey, Person } from '@mui/icons-material'
import { useSessionInfo } from './RequireConfig'
import { Avatar, SvgIconProps, AvatarProps, Skeleton } from '@mui/material'

interface UserAvatarProps extends AvatarProps {
  userID: string
}

function useAvatar(
  Fallback: React.FC<SvgIconProps>,
  otherProps: AvatarProps,
  loading = false,
): JSX.Element {
  const av = (
    <Avatar alt='' data-cy='avatar-fallback' {...otherProps}>
      <Fallback color='primary' />
    </Avatar>
  )

  if (loading) {
    return <Skeleton variant='circular'>{av}</Skeleton>
  }

  return av
}

export function UserAvatar(props: UserAvatarProps): JSX.Element {
  const otherProps = { ...props }
  delete (otherProps as Partial<UserAvatarProps>).userID
  return useAvatar(Person, otherProps)
}

export function CurrentUserAvatar(props: AvatarProps): JSX.Element {
  const { ready } = useSessionInfo()
  return useAvatar(Person, props, !ready)
}

export function ServiceAvatar(props: AvatarProps): JSX.Element {
  return useAvatar(VpnKey, props)
}

export function EPAvatar(props: AvatarProps): JSX.Element {
  return useAvatar(Layers, props)
}

export function RotationAvatar(props: AvatarProps): JSX.Element {
  return useAvatar(RotateRight, props)
}

export function ScheduleAvatar(props: AvatarProps): JSX.Element {
  return useAvatar(Today, props)
}
