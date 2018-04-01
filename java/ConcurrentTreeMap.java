
public interface ConcurrentTreeMap<K, V> {
	
	public int size();
	
	public V get(final K key);
	
	public V put(final K key, final V value);
	
	public V remove(final K key);
	
}
