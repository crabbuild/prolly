import { useCallback, useEffect, useId, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

interface InfoTipProps {
  children: string;
  dark?: boolean;
}

export function InfoTip({ children, dark = false }: InfoTipProps) {
  const id = useId();
  const triggerRef = useRef<HTMLSpanElement>(null);
  const [open, setOpen] = useState(false);
  const [position, setPosition] = useState({ left: 0, top: 0, above: false });

  const updatePosition = useCallback(() => {
    const trigger = triggerRef.current;
    if (!trigger) return;
    const rect = trigger.getBoundingClientRect();
    const halfWidth = Math.min(130, (window.innerWidth - 24) / 2);
    const left = Math.min(window.innerWidth - 12 - halfWidth, Math.max(12 + halfWidth, rect.left + rect.width / 2));
    const above = rect.bottom + 90 > window.innerHeight && rect.top > 90;
    setPosition({ left, top: above ? rect.top - 8 : rect.bottom + 8, above });
  }, []);

  useEffect(() => {
    if (!open) return;
    const reposition = () => updatePosition();
    window.addEventListener('resize', reposition);
    window.addEventListener('scroll', reposition, true);
    return () => {
      window.removeEventListener('resize', reposition);
      window.removeEventListener('scroll', reposition, true);
    };
  }, [open, updatePosition]);

  const show = () => {
    updatePosition();
    setOpen(true);
  };

  return (
    <span ref={triggerRef} className={dark ? 'info-tip dark' : 'info-tip'} tabIndex={0} aria-label="More information" aria-describedby={id} onMouseEnter={show} onMouseLeave={() => setOpen(false)} onFocus={show} onBlur={() => setOpen(false)}>
      <span className="info-tip-mark" aria-hidden="true">?</span>
      {open && createPortal(
        <span className={position.above ? 'info-tip-content above' : 'info-tip-content'} id={id} role="tooltip" style={{ left: position.left, top: position.top }}>{children}</span>,
        document.body,
      )}
    </span>
  );
}
