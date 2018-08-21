/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.ddb.efdumper;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Michael Büchner <m.buechner@dnb.de>
 */
class EFDScheduler {

    private List<Thread> threads = new ArrayList<>();
    private static EFDScheduler instance = null;

    private EFDScheduler() {
    }

    public static EFDScheduler getInstance() {
        if (instance == null) {
            instance = new EFDScheduler();
        }
        return instance;
    }

    public boolean canAdd(int size) {
        clean();
        return threads.size() + size < App.MAXTHREADS;
    }

    public boolean canAdd() {
        clean();
        return threads.size() < App.MAXTHREADS;
    }

    public boolean isDone() {
        clean();
        return this.threads.size() < 1;
    }

    public boolean add(Thread thread) {
        if (!canAdd()) {
            return false;
        }

        final boolean ret = this.threads.add(thread);
        thread.start();
        return ret;
    }

    private void clean() {
        final List<Thread> tmp = new ArrayList<>();
        for (int i = 0; i < threads.size(); ++i) {
            Thread thread = threads.get(i);
            if (thread.isAlive()) {
                tmp.add(thread);
            }
        }
        this.threads = tmp;
    }

    public boolean remove(Thread thread) {
        return this.threads.remove(thread);
    }
}
