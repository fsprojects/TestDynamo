namespace DynamoDbInMemory.Model

open DynamoDbInMemory.Utils

type SynchronizationErrorType =
    | AddedIndexConflict

type SynchronizationException(fromDb: DatabaseId, toDb: DatabaseId, subscriberId: IncrementingId, errorType, inner) =
    inherit System.Exception(
        message = System.String.Format(format = SynchronizationException.getMessage errorType, arg0 = toDb, arg1 = fromDb, arg2 = subscriberId), innerException = inner)

    with
    member _.SubscriberId = subscriberId
    member _.FromDb = fromDb
    member _.ToDb = toDb

    static member private getMessage = function
        | AddedIndexConflict -> SynchronizationException.addedIndexConflictMessage

    static member private addedIndexConflictMessage =
            [
                [
                    "The database {0} has encountered an error when synchronizing from {1}."
                    "Synchronization errors occur when a new index is added to a global table"
                    "and it is not possible to replicate this index because it conflicts "
                    "with an item in the downstream table."
                ] |> Str.join " "
                [
                    "It is not possible to recover from this type of error until the modified table"
                    "is disconnected from its downstream, by deleting the replica using the UpdateTable or UpdateGlobalTable methods."
                ] |> Str.join " "
                [
                    "To avoid this issue in the future, ensure that all indexes are added to a global table before adding any data,"
                    "or ensure that no data replication is in progress while adding an index to a global table."
                ] |> Str.join " "
                [
                    "Subscriber {2}"
                ] |> Str.join " "
            ]
            |> Str.join "\n"