def logout(self) -> None:
    """
    Log out the user by clearing the access token.
    """
    self.token = None
