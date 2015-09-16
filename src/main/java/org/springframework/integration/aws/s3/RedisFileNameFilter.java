package org.springframework.integration.aws.s3;


import java.io.Flushable;
import java.io.IOException;

import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.util.Assert;

/**
 * Filters out the files by matching the given File name against the Redis MetaDataStore
 *
 * @author Li Wang
 *
 */
public class RedisFileNameFilter extends AbstractFileNameFilter {
	
	protected final ConcurrentMetadataStore store;
	protected final Flushable flushableStore;
	private final Object monitor = new Object();

	public RedisFileNameFilter(ConcurrentMetadataStore store) {
		Assert.notNull(store, "'store' cannot be null");
		this.store = store;
		if (store instanceof Flushable) {
			this.flushableStore = (Flushable) store;
		} else {
			this.flushableStore = null;
		}
	}

	@Override
	public boolean isFileNameAccepted(String fileName) {
		String key = fileName;
		synchronized(monitor) {
			String newValue = fileName;
			String oldValue = this.store.putIfAbsent(key, newValue);
			if (oldValue == null) { // not in store
				flushIfNeeded();
				return true;
			}			
			return false;
		}
	}
	
	protected void flushIfNeeded() {
		if (this.flushableStore != null) {
			try {
				this.flushableStore.flush();
			}
			catch (IOException e) {
				// store's responsibility to log
			}
		}
	}

}
