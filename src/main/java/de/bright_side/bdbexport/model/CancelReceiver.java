package de.bright_side.bdbexport.model;

public class CancelReceiver {
	private boolean cancel = false;

	public boolean wantToCancel() {
		return cancel;
	}

	public void cancel() {
		this.cancel = true;
	}

}
