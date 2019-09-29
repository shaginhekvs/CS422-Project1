package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.DataType;

public class Key<T>{
	T element;
	DataType dt;
	public Key(T a, DataType dt){
		element = a;
		this.dt=dt;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime;
		switch (dt) {
		case BOOLEAN:
		case INT:
		case DOUBLE:
		case STRING:
			result+= ((String)element).hashCode();
		}
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Key other = (Key) obj;
		if (element == null) {
			if (other.element != null)
				return false;
		} else if (!((String)element).equals((String)other.element))
			return false;
		return true;
	}
	
}
