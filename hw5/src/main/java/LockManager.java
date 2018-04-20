import com.sun.org.apache.regexp.internal.RE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;

    public LockManager() {
        this.resourceToLock = new HashMap<Resource, ResourceLock>();

    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {
        // HW5: To do
        if (transaction.getStatus() == Transaction.Status.Waiting) { // blocked transaction
            throw new IllegalArgumentException("Blocked transaction is attempting to acquire lock");
        }

        if (resourceToLock.get(resource) == null) {
            resourceToLock.put(resource, new ResourceLock());
        }
        ResourceLock rl = resourceToLock.get(resource);
        Request req = new Request(transaction, lockType);
        if (compatible(resource, transaction, lockType)) {
            rl.lockOwners.add(req);
        } else {
            rl.requestersQueue.add(req);
            transaction.sleep();
        }
        return;
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {
        // HW5: To do
        ResourceLock rl = resourceToLock.get(resource);
//        if (rl == null) return true;
        if (lockType == LockType.S) {
            for (Request req : rl.lockOwners) {
                if (req.lockType == LockType.S && req.transaction == transaction) {
                    throw new IllegalArgumentException("Requested already held lock S");
                }
                if (req.lockType == LockType.X && req.transaction == transaction) {
                    throw new IllegalArgumentException("Attempted downgrade from X to S");
                }
                if (req.lockType == LockType.IX || req.lockType == LockType.X) {
                    return false;
                }
            }
            return true;

        } else if (lockType == LockType.X) {
            if (rl.lockOwners.isEmpty()) {
                return true;
            } else {
                for (Request req : rl.lockOwners) {
                    if (req.lockType == LockType.X && req.transaction == transaction) {
                        throw new IllegalArgumentException("Requested already held lock X");
                    }
                }
            }
            return false;

        } else if (lockType == lockType.IS) {
            if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                throw new IllegalArgumentException("Requested IS on page");
            }
            for (Request req : rl.lockOwners) {
                if (req.lockType == LockType.IS && req.transaction == transaction) {
                    throw new IllegalArgumentException("Requested already held lock IS");
                }
                if (req.lockType == LockType.IX && req.transaction == transaction) {
                    throw new IllegalArgumentException("Attempted downgrade from IX to IS");
                }
                if (req.lockType == LockType.X) {
                    return false;
                }
            }
            return true;

        } else {
            if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                throw new IllegalArgumentException("Requested IX on page");
            }
            for (Request req : rl.lockOwners) {
                if (req.lockType == LockType.IX && req.transaction == transaction) {
                    throw new IllegalArgumentException("Requested already held lock IX");
                }
                if (req.lockType == LockType.S || req.lockType == LockType.X) {
                    return false;
                }
            }
            return true;

        }
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{
        // HW5: To do
        boolean holdsLock = false;
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException("Blocked transaction is attempting to release lock");
        }
        if (resourceToLock.get(resource) == null) {
            resourceToLock.put(resource, new ResourceLock());
        }
        ResourceLock rl = resourceToLock.get(resource);
        ArrayList<Request> locks = new ArrayList<>(rl.lockOwners);
        for (Request req : locks) {
            if (req.transaction == transaction) {
                rl.lockOwners.remove(req); // release request
                holdsLock = true;
            }
        }
        if (!holdsLock) {
            throw new IllegalArgumentException("Transaction doesn't hold any of the four possible lock types on this resource");
        }
        rl.lockOwners = locks;
        promote(resource);
        return;
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         // HW5: To do
         ResourceLock rl = resourceToLock.get(resource);
         for (Request req : rl.requestersQueue) {
             if (compatible(resource, req.transaction, req.lockType)) {
                 Request request = rl.requestersQueue.pop();
                 rl.lockOwners.add(request);
                 request.transaction.wake();
             } else {
                 return;
             }
         }
         return;
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        // HW5: To do
        if (resourceToLock.get(resource) == null) {
            resourceToLock.put(resource, new ResourceLock());
        }
        ResourceLock rl = resourceToLock.get(resource);
        for (Request req : rl.lockOwners) {
            if (req.transaction == transaction && req.lockType == lockType) {
                return true;
            }
        }
        return false;
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}
