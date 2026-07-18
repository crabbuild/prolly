# Prolly MySQL store for Swift

`MySQLRemoteStore` implements the shared asynchronous store protocol with
MySQLNIO 1.8, the newest release compatible with Swift 5.10. It borrows an open
`MySQLConnection` and never closes the connection.
