using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AnimEvent : MonoBehaviour
{
    private Animator ani;

    private void Start()
    {
        ani = GetComponent<Animator>();
    }

    public void LiftEvent()
    {
        ani.SetBool(Animator.StringToHash("Rise"), true);
        ani.SetTrigger(Animator.StringToHash("Lift"));
    }

    public void LowerEvent()
    {
        ani.SetBool(Animator.StringToHash("Rise"), false);
        ani.SetTrigger(Animator.StringToHash("Lower"));
    }
}
