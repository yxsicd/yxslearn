package com.yxs;

import java.text.MessageFormat;

public class DosAttackRecord
{
    private static final long   LOCK_COUNT = 5;
    private static final long   CHECK_SPAN = 7 * 1000;
    private static final double LOCK_VALUE = (double) LOCK_COUNT / CHECK_SPAN;
    private static final double LOCK_SPAN = CHECK_SPAN * 2;

    private long                attackCount;
    private long                firstAttackTime;
    private long                lockTime;

    public DosAttackRecord()
    {
        this.attackCount = 0;
        this.firstAttackTime = System.currentTimeMillis();
        this.lockTime = System.currentTimeMillis();
    }

    public boolean addAttack()
    {
        attackCount++;
        long lastAttackTime = System.currentTimeMillis();

        boolean ret = false;
        if (lastAttackTime < lockTime)
        {
            reset();
            ret = true;
        }
        else if (attackCount > LOCK_COUNT)
        {
            if (lastAttackTime - firstAttackTime < CHECK_SPAN)
            {
                lock();
                ret = true;
            }
            else
            {
                reset();
                ret = false;
            }
        }

        return ret;

    }

    public void reset()
    {
        
        System.out.println(MessageFormat.format("pre-reset now C={0} AT={1} LT={2}",
                Long.toString(this.attackCount),
                Long.toString(this.firstAttackTime),
                Long.toString(this.lockTime)));
        this.attackCount = 0;
        this.firstAttackTime = System.currentTimeMillis();
        System.out.println(MessageFormat.format("post-reset now C={0} AT={1} LT={2}",
                Long.toString(this.attackCount),
                Long.toString(this.firstAttackTime),
                Long.toString(this.lockTime)));
    }

    public void lock()
    {
        this.lockTime = System.currentTimeMillis() + CHECK_SPAN * 2;
        System.out.println(MessageFormat.format("lock C={0} AT={1} LT={2}",
                Long.toString(this.attackCount),
                Long.toString(this.firstAttackTime),
                Long.toString(this.lockTime)));
        this.attackCount = 0;

    }

    public static void main(String[] args)
    {
        DosAttackRecord daRecord = new DosAttackRecord();
        for (int i = 0; i < 121; i++)
        {
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            System.out.println(daRecord.addAttack());
        }
    }

}