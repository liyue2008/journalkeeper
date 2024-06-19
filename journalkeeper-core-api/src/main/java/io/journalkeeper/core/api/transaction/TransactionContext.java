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
package io.journalkeeper.core.api.transaction;

import java.util.Map;

/**
 * Transaction context.
 * The context of a transaction is provided by user while create the transaction,
 * and it will write into the transaction prepare log.
 *
 * @author LiYue
 * Date: 2019/11/29
 */
public interface TransactionContext {
    TransactionId transactionId();

    Map<String, String> context();

    long timestamp();
}
