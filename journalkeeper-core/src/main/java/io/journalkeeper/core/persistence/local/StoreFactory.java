/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.persistence.local;

import io.journalkeeper.core.persistence.local.journal.PositioningStore;
import io.journalkeeper.core.persistence.local.metadata.JsonDoubleCopiesPersistence;
import io.journalkeeper.core.persistence.JournalPersistence;
import io.journalkeeper.core.persistence.LockablePersistence;
import io.journalkeeper.core.persistence.MetadataPersistence;
import io.journalkeeper.core.persistence.PersistenceFactory;
import io.journalkeeper.core.persistence.local.lock.FileLock;

import java.nio.file.Path;

public class StoreFactory implements PersistenceFactory {

    @Override
    public MetadataPersistence createMetadataPersistenceInstance() {
        return new JsonDoubleCopiesPersistence();
    }

    @Override
    public JournalPersistence createJournalPersistenceInstance() {
        return new PositioningStore();
    }

    @Override
    public LockablePersistence createLock(Path lockPath) {
        return new FileLock(lockPath);
    }

}